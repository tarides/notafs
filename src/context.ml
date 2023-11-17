module type DISK_EXTENSIONS = sig
  (* to remove, only used for [block_viz] debugging *)
  type t

  val discard : t -> Int64.t -> unit
  val flush : t -> unit
end

module type DISK = sig
  include Mirage_block.S
  include DISK_EXTENSIONS with type t := t
end

module type SIMPLE_DISK = sig
  type error
  type write_error
  type t

  val pp_error : Format.formatter -> error -> unit
  val pp_write_error : Format.formatter -> write_error -> unit
  val get_info : t -> Mirage_block.info Lwt.t
  val read : t -> int64 -> Cstruct.t list -> (unit, error) result Lwt.t
  val write : t -> int64 -> Cstruct.t list -> (unit, write_error) result Lwt.t

  include DISK_EXTENSIONS with type t := t
end

module type CHECKSUM = sig
  type t

  val name : string
  (** A short [name] (max 8 characters) to identify the checksum algorithm. *)

  val equal : t -> t -> bool
  val default : t
  val of_int32 : Int32.t -> t
  val to_int32 : t -> Int32.t
  val digest_bigstring : Checkseum.bigstring -> int -> int -> t -> t

  include Id.FIELD with type t := t
end

module type A_DISK = sig
  module Id : Id.S
  module C : CHECKSUM

  val stats : Stats.t
  val dirty : bool ref

  type read_error
  type write_error

  type error =
    [ `Read of read_error
    | `Write of write_error
    | `Invalid_checksum of Int64.t
    | `All_generations_corrupted
    | `Disk_not_formatted
    | `Disk_is_full
    | `Wrong_page_size of int
    | `Wrong_disk_size of Int64.t
    | `Wrong_checksum_algorithm of string * int
    ]

  val pp_error : Format.formatter -> error -> unit
  val page_size : int
  val header_size : int
  val nb_sectors : int64

  type sector

  val set_id : sector Lru.elt -> Id.t -> unit
  val lru : sector Lru.t
  val cstruct : sector Lru.elt -> (Cstruct.t, [> `Read of read_error ]) Lwt_result.t
  val cstruct_in_memory : sector Lru.elt -> Cstruct.t
  val read : Id.t -> Cstruct.t -> (unit, [> `Read of read_error ]) Lwt_result.t
  val write : Id.t -> Cstruct.t list -> (unit, [> `Write of write_error ]) Lwt_result.t
  val discard : Id.t -> unit
  val acquire_discarded : unit -> Id.t list
  val flush : unit -> unit
  val allocator : (int -> (Id.t list, error) Lwt_result.t) ref
  val safe_lru : bool ref
  val allocate : from:[ `Root | `Load ] -> unit -> (sector Lru.elt, error) Lwt_result.t
  val unallocate : sector Lru.elt -> unit

  val set_finalize
    :  sector
    -> (unit
        -> ((int * (Id.t -> (unit, error) Lwt_result.t), Id.t) result, error) Lwt_result.t)
    -> unit
end

let of_impl
  (type t e we)
  (module B : SIMPLE_DISK with type t = t and type error = e and type write_error = we)
  (module C : CHECKSUM)
  (disk : t)
  =
  let open Lwt.Syntax in
  let+ info = B.get_info disk in
  (module struct
    module Id = (val Id.of_nb_pages info.size_sectors)
    module C = C

    let stats = Stats.create ()

    type page =
      | Cstruct of Cstruct.t
      | On_disk of Id.t
      | Freed

    let dirty = ref false

    type read_error = B.error
    type write_error = B.write_error

    type error =
      [ `Read of read_error
      | `Write of write_error
      | `Invalid_checksum of Int64.t
      | `All_generations_corrupted
      | `Disk_is_full
      | `Disk_not_formatted
      | `Wrong_page_size of int
      | `Wrong_disk_size of Int64.t
      | `Wrong_checksum_algorithm of string * int
      ]

    let pp_error h = function
      | `Read e -> B.pp_error h e
      | `Write e -> B.pp_write_error h e
      | `Invalid_checksum id ->
        Format.fprintf h "Invalid_checksum %s" (Int64.to_string id)
      | `All_generations_corrupted -> Format.fprintf h "All_generations_corrupted"
      | `Disk_not_formatted -> Format.fprintf h "Disk_not_formatted"
      | `Disk_is_full -> Format.fprintf h "Disk_is_full"
      | `Wrong_page_size s -> Format.fprintf h "Wrong_page_size %d" s
      | `Wrong_disk_size i -> Format.fprintf h "Wrong_disk_size %s" (Int64.to_string i)
      | `Wrong_checksum_algorithm (s, i) ->
        Format.fprintf h "Wrong_checksum_algorithm (%s, %d)" s i
      | `Unsupported_operation msg -> Format.fprintf h "Unsupported_operation %S" msg
      | `Disk_failed -> Format.fprintf h "Disk_failed"

    type sector =
      { mutable cstruct : page
      ; mutable finalize :
          unit
          -> ( (int * (Id.t -> (unit, error) Lwt_result.t), Id.t) result
             , error )
             Lwt_result.t
      }

    let header_size = 1
    let page_size = info.sector_size
    let nb_sectors = info.size_sectors
    let concurrent_reads = ref []

    let rec regroup (first, last, cs, acc) = function
      | [] -> List.rev ((first, List.rev cs) :: acc)
      | (id, c) :: rest ->
        if Id.(equal (succ last) id)
        then regroup (first, id, c :: cs, acc) rest
        else regroup (id, id, [ c ], (first, List.rev cs) :: acc) rest

    let regroup = function
      | [] -> invalid_arg "Root.regroup: empty list"
      | (id, c) :: rest -> regroup (id, id, [ c ], []) rest

    let regroup lst =
      regroup @@ List.sort (fun (a_id, _) (b_id, _) -> Id.compare a_id b_id) lst

    let read page_id cstruct =
      let is_done, set_done = Lwt.wait () in
      let is_done =
        let+ r = is_done in
        match r with
        | Ok () -> Ok ()
        | Error e -> Error (`Read e)
      in
      concurrent_reads := (page_id, (cstruct, set_done)) :: !concurrent_reads ;
      let* () = Lwt.pause () in
      begin
        match !concurrent_reads with
        | [] -> is_done
        | to_read ->
          concurrent_reads := [] ;
          let groups = regroup to_read in
          let* () =
            Lwt_list.iter_p
              (fun (page_id, cstructs_complete) ->
                let cstructs = List.map fst cstructs_complete in
                let page_id = Id.to_int64 page_id in
                Stats.incr_read stats (List.length cstructs) ;
                let+ r = B.read disk page_id cstructs in
                List.iter
                  (fun (_, set_done) -> Lwt.wakeup_later set_done r)
                  cstructs_complete)
              groups
          in
          is_done
      end

    let write page_id cstructs =
      Stats.incr_write stats (List.length cstructs) ;
      let page_id = Id.to_int64 page_id in
      let open Lwt.Syntax in
      let+ result = B.write disk page_id cstructs in
      Result.map_error (fun e -> `Write e) result

    let discarded = ref []

    let discard page_id =
      discarded := page_id :: !discarded ;
      let page_id = Id.to_int64 page_id in
      B.discard disk page_id

    let acquire_discarded () =
      let lst = !discarded in
      discarded := [] ;
      List.rev lst

    let flush () = B.flush disk
    let allocator = ref (fun _ -> failwith "no allocator")
    let lru = Lru.make ()
    let safe_lru = ref true
    let available_cstructs = ref []
    let release_cstructs cstructs = available_cstructs := cstructs :: !available_cstructs

    let unallocate elt =
      let t = Lru.value elt in
      begin
        match t.cstruct with
        | Cstruct cstruct ->
          release_cstructs [ cstruct ] ;
          t.cstruct <- Freed
        | On_disk _id -> ()
        | Freed -> failwith "Context.unallocate Freed"
      end ;
      Lru.detach elt lru

    let set_id elt id =
      let t = Lru.value elt in
      begin
        match t.cstruct with
        | Cstruct cstruct ->
          release_cstructs [ cstruct ] ;
          t.cstruct <- On_disk id
        | On_disk id' -> assert (Id.equal id id')
        | Freed -> failwith "Context.set_id: Freed"
      end ;
      Lru.detach_remove elt lru

    let max_lru_size = 2048
    let min_lru_size = max_lru_size / 2

    let rec write_all = function
      | [] -> Lwt_result.return ()
      | (id, cs) :: css ->
        let open Lwt_result.Syntax in
        let id_ = Int64.to_int @@ Id.to_int64 id in
        assert (id_ <> 0 && id_ <> 1) ;
        let* () = write id cs in
        write_all css

    let write_all lst = write_all (regroup lst)
    let no_finalizer _ = failwith "no finalizer"

    let rec list_align_with acc xs ys =
      match xs, ys with
      | rest, [] -> List.rev acc, rest
      | x :: xs, _ :: ys -> list_align_with (x :: acc) xs ys
      | _ -> assert false

    let rec lru_make_room acc =
      let open Lwt_result.Syntax in
      if Lru.length lru < min_lru_size
         ||
         match Lru.peek_back lru with
         | None -> assert false
         | Some e when e.finalize == no_finalizer -> true
         | _ -> false
      then begin
        match acc with
        | [] -> Lwt_result.return ()
        | _ -> begin
          let nb = List.length acc in
          let* ids = !allocator nb in
          let acc =
            List.filter
              (fun (s, _, _) ->
                match s.cstruct with
                | Cstruct _ -> true
                | _ -> false)
              acc
          in
          let ids, ids_rest = list_align_with [] ids acc in
          List.iter discard ids_rest ;
          let acc =
            List.sort
              (fun (_, a_depth, _) (_, b_depth, _) -> Int.compare b_depth a_depth)
              acc
          in
          let rec finalize acc ids ss =
            match ids, ss with
            | [], [] -> Lwt_result.return acc
            | id :: ids, (s, _, finalizer) :: ss ->
              let* cstruct =
                match s.cstruct with
                | Cstruct cstruct ->
                  let+ () = finalizer id in
                  s.cstruct <- On_disk id ;
                  id, cstruct
                | On_disk _ -> assert false
                | Freed -> assert false
              in
              finalize (cstruct :: acc) ids ss
            | _ -> assert false
          in
          let* cstructs = finalize [] ids acc in
          let+ () = write_all cstructs in
          let rec finalize ids acc =
            match ids, acc with
            | [], [] -> ()
            | id :: ids, (s, _, _) :: ss ->
              begin
                match s.cstruct with
                | On_disk id' -> assert (id = id')
                | Cstruct _ -> failwith "Context.finalize: Cstruct"
                | Freed -> failwith "Context.finalize: Freed"
              end ;
              finalize ids ss
            | _ -> assert false
          in
          finalize ids acc ;
          release_cstructs (List.map snd cstructs) ;
          flush ()
        end
      end
      else
        let* acc =
          match Lru.pop_back lru with
          | None -> assert false
          | Some old -> begin
            match old.cstruct with
            | Freed -> failwith "Cstruct.lru_make_room: Freed"
            | On_disk _ -> Lwt_result.return acc
            | Cstruct _cstruct -> begin
              let* fin = old.finalize () in
              match fin with
              | Error page_id ->
                old.cstruct <- On_disk page_id ;
                (*
                   let fake_cstruct = Cstruct.create page_size in
                   let* () = read page_id fake_cstruct in
                   if not (Cstruct.to_string cstruct = Cstruct.to_string fake_cstruct)
                   then begin
                   Format.printf "===== SECTOR %s =====@." (Id.to_string page_id) ;
                   Format.printf "EXPECTED %S@." (Cstruct.to_string cstruct) ;
                   Format.printf "     GOT %S@." (Cstruct.to_string fake_cstruct) ;
                   failwith "inconsistent"
                   end ;
                *)
                Lwt_result.return acc
              | Ok (depth, finalizer) -> Lwt_result.return ((old, depth, finalizer) :: acc)
            end
          end
        in
        lru_make_room acc

    let cstruct_reset c =
      Cstruct.memset c 0xFF ;
      c

    let cstruct_create () =
      match !available_cstructs with
      | [] -> Cstruct.create page_size
      | [ c ] :: css ->
        available_cstructs := css ;
        cstruct_reset c
      | [] :: _ -> assert false
      | (c :: cs) :: css ->
        available_cstructs := cs :: css ;
        cstruct_reset c

    let allocate ~from () =
      let sector = { cstruct = Cstruct (cstruct_create ()); finalize = no_finalizer } in
      match from with
      | `Root -> Lwt_result.return (Lru.make_detached sector)
      | `Load -> begin
        let open Lwt_result.Syntax in
        let make_room () =
          if (not !safe_lru) || Lru.length lru < max_lru_size
          then Lwt_result.return ()
          else begin
            assert !safe_lru ;
            safe_lru := false ;
            let+ () = lru_make_room [] in
            safe_lru := true
          end
        in
        let+ () = make_room () in
        Lru.make_elt sector lru
      end

    let set_finalize s fn = s.finalize <- fn

    let cstruct_in_memory elt =
      let sector = Lru.value elt in
      match sector.cstruct with
      | Cstruct cstruct -> cstruct
      | On_disk _ -> failwith "Context.cstruct_in_memory: On_disk"
      | Freed -> failwith "Context.cstruct_in_memory: Freed"

    let cstruct elt =
      Lru.use elt lru ;
      let sector = Lru.value elt in
      match sector.cstruct with
      | Freed -> failwith "Context.cstruct: Freed"
      | Cstruct cstruct -> Lwt_result.return cstruct
      | On_disk page_id ->
        let cstruct = cstruct_create () in
        let open Lwt_result.Syntax in
        let+ () = read page_id cstruct in
        sector.cstruct <- Cstruct cstruct ;
        cstruct
  end : A_DISK
    with type read_error = e
     and type write_error = we)

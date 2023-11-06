module Make (B : Context.A_DISK) : sig
  type id = B.Id.t

  val id_size : int
  val id_t : id Repr.t
  val null_id : id
  val is_null_id : id -> bool

  type ptr

  val ptr_t : ptr Repr.t
  val null_ptr : ptr
  val is_null_ptr : ptr -> bool
  val ptr_size : int

  type t

  val force_id : t -> id
  val to_ptr : t -> ptr

  type loc
  type 'a r := ('a, B.error) Lwt_result.t

  val root_loc : id -> loc
  val create : ?at:loc -> unit -> t r
  val load_root : ?check:bool -> id -> t r
  val load : ptr -> t r
  val write_root : t -> unit r
  val to_write : t -> (id * Cstruct.t) r
  val length : t -> int
  val get_uint8 : t -> int -> int r
  val set_uint8 : t -> int -> int -> unit r
  val get_uint16 : t -> int -> int r
  val set_uint16 : t -> int -> int -> unit r
  val get_uint32 : t -> int -> int r
  val set_uint32 : t -> int -> int -> unit r
  val get_uint64 : t -> int -> Int64.t r
  val set_uint64 : t -> int -> Int64.t -> unit r
  val read_id : t -> int -> id r
  val write_id : t -> int -> id -> unit r
  val get_child : t -> int -> t r
  val set_child : t -> int -> t -> unit r
  val get_child_ptr : t -> int -> ptr r
  val set_child_ptr : t -> int -> ptr -> unit r
  val erase_region : t -> off:int -> len:int -> unit r
  val blit_from_string : string -> int -> t -> int -> int -> unit r
  val blit_to_bytes : t -> int -> bytes -> int -> int -> unit r

  (* *)
  val count_new : t -> int r
  val finalize : t -> id list -> ((id * Cstruct.t) list * id list) r
  val verify_checksum : t -> unit r
  val drop_release : t -> unit
  val is_in_memory : t -> bool
end = struct
  module H = Hashtbl.Make (struct
      type t = int

      let equal = Int.equal
      let hash = Hashtbl.hash
    end)

  module C = B.C

  type id = B.Id.t

  let id_t : id Repr.t = B.Id.t

  type loc =
    | Root of id
    | At of id
    | In_memory
    | Freed

  let[@warning "-32"] string_of_loc = function
    | Root id -> "(Root " ^ B.Id.to_string id ^ ")"
    | At id -> "(At " ^ B.Id.to_string id ^ ")"
    | In_memory -> "In_memory"
    | Freed -> "Freed"

  type t =
    { mutable id : loc
    ; mutable checksum : C.t option
    ; cstruct : B.sector Lru.elt
    ; children : t H.t
    ; mutable parent : parent
    ; mutable depth : int
    }

  and parent =
    | Detached
    | Parent of t * int

  type ptr =
    | Disk of id * C.t
    | Mem of t

  let cs_t : C.t Repr.t =
    Repr.map Repr.int32 (fun cs -> C.of_int32 cs) (fun cs -> C.to_int32 cs)

  let ptr_t : ptr Repr.t =
    Repr.map
      Repr.(pair id_t cs_t)
      (fun (id, cs) -> Disk (id, cs))
      (function
       | Disk (id, cs) -> id, cs
       | _ -> invalid_arg "Sector.ptr_t: serialize Mem")

  let to_ptr t =
    match t.id with
    | Root id | At id ->
      let cs = Option.get t.checksum in
      Disk (id, cs)
    | In_memory -> invalid_arg "Sector.to_ptr: not allocated"
    | Freed -> invalid_arg "Sector.to_ptr: freed"

  let null_id = B.Id.of_int 0
  let id_size = B.Id.byte_size
  let null_cs = C.default
  let cs_size = C.byte_size
  let ptr_size = id_size + cs_size
  let is_null_id id = B.Id.equal null_id id
  let is_null_cs cs = C.equal null_cs cs
  let null_ptr = Disk (null_id, null_cs)

  let is_null_ptr = function
    | Disk (id, cs) -> is_null_id id && is_null_cs cs
    | Mem _ -> false

  let cstruct_to_bigarray cstruct =
    let open Cstruct in
    assert (cstruct.off = 0) ;
    assert (cstruct.len = B.page_size) ;
    assert (cstruct.len = Bigarray.Array1.dim cstruct.buffer) ;
    cstruct.buffer

  let get_checksum cstruct =
    C.digest_bigstring (Cstruct.to_bigarray cstruct) 0 B.page_size C.default

  let set_checksum cstruct offset cs = C.write cstruct (offset + id_size) cs
  let read_checksum cstruct offset = C.read cstruct (offset + id_size)

  open Lwt_result.Syntax

  let ro_cstruct t = B.cstruct t.cstruct
  let root_checksum_offset = B.page_size - C.byte_size

  let compute_root_checksum cstruct =
    C.write cstruct root_checksum_offset C.default ;
    C.digest_bigstring (cstruct_to_bigarray cstruct) 0 B.page_size C.default

  let check_root_checksum ~id cstruct =
    let* cstruct = B.cstruct cstruct in
    let cs = C.read cstruct root_checksum_offset in
    let expected = compute_root_checksum cstruct in
    if C.equal cs expected
    then Lwt_result.return ()
    else Lwt_result.fail (`Invalid_checksum id)

  let verify_checksum t =
    match t.id, t.checksum with
    | At id, Some cs ->
      let* cstruct = ro_cstruct t in
      let cs' = get_checksum cstruct in
      if C.equal cs cs'
      then Lwt_result.return ()
      else Lwt_result.fail (`Invalid_checksum id)
    | Root id, None -> check_root_checksum ~id t.cstruct
    | In_memory, None -> Lwt_result.return ()
    | _ -> assert false

  let root_loc i = Root i

  let is_in_memory t =
    match t.id with
    | At _ -> false
    | Freed -> assert false
    | _ -> true

  let ptr_of_t t =
    match t.id with
    | Root id | At id ->
      let cs = Option.get t.checksum in
      Disk (id, cs)
    | In_memory -> Mem t
    | Freed -> invalid_arg "Sector.ptr_of_t: freed"

  let rw_cstruct t =
    let+ cs = B.cstruct t.cstruct in
    assert (is_in_memory t) ;
    assert (t.checksum = None) ;
    cs

  let rec release t =
    match t.id with
    | Freed -> failwith "Sector.release: Freed"
    | In_memory | Root _ -> Lwt_result.return ()
    | At id ->
      B.discard id ;
      t.id <- In_memory ;
      t.checksum <- None ;
      begin
        match t.parent with
        | Detached -> Lwt_result.return ()
        | Parent (parent, offset) -> set_child parent offset t
      end

  and set_child_ptr t offset = function
    | Disk (ptr, cs) ->
      let* () = release t in
      let+ cstruct = rw_cstruct t in
      B.Id.write cstruct offset ptr ;
      set_checksum cstruct offset cs ;
      begin
        match H.find_opt t.children offset with
        | None -> ()
        | Some child -> assert (child.id = At ptr)
      end
    | Mem child ->
      let+ () = release t in
      assert (is_in_memory t) ;
      begin
        match H.find_opt t.children offset with
        | None -> H.replace t.children offset child
        | Some self -> assert (self == child)
      end ;
      assert (H.mem t.children offset)

  and set_child t offset child =
    begin
      match child.parent with
      | Parent (t', _) when t == t' ->
        assert (child.depth = t.depth + 1) ;
        ()
      | Detached ->
        if H.length t.children = 0
        then begin
          assert (t.parent = Detached) ;
          assert (t.depth = 0) ;
          t.depth <- child.depth - 1
        end
        else begin
          assert (H.length child.children = 0) ;
          assert (child.depth = 0) ;
          child.depth <- t.depth + 1
        end
      | _ -> failwith "set_child: would lose parent"
    end ;
    child.parent <- Parent (t, offset) ;
    let ptr = ptr_of_t child in
    let+ () = set_child_ptr t offset ptr in
    begin
      match ptr with
      | Mem _ -> ()
      | Disk _ -> begin
        match H.find t.children offset with
        | self -> assert (self == child)
        | exception Not_found -> H.replace t.children offset child
      end
    end

  let finalize_set_id t () =
    match t.id with
    | Root _ -> failwith "Sector.finalize_set_id: Root"
    | Freed -> failwith "Sector.finalize_set_id: Freed"
    | At page_id ->
      let+ () =
        match t.parent with
        | Detached -> Lwt_result.return ()
        | Parent (parent, offset) ->
          begin
            try
              let self = H.find parent.children offset in
              assert (self == t)
            with
            | Not_found -> ()
          end ;
          let+ parent_cstruct = ro_cstruct parent in
          let ptr = B.Id.read parent_cstruct offset in
          assert (B.Id.equal ptr page_id) ;
          let cs = read_checksum parent_cstruct offset in
          assert (C.equal cs (Option.get t.checksum))
      in
      Error page_id
    | In_memory ->
      Lwt_result.return
      @@ Ok
           ( t.depth
           , fun id ->
               assert (t.id = In_memory) ;
               t.id <- At id ;
               let cstruct = B.cstruct_in_memory t.cstruct in
               let cs = get_checksum cstruct in
               t.checksum <- Some cs ;
               begin
                 match t.parent with
                 | Detached -> Lwt_result.return ()
                 | Parent (parent, offset) ->
                   begin
                     match H.find parent.children offset with
                     | exception Not_found -> failwith "trying to overwrite parent"
                     | child -> assert (child == t)
                   end ;
                   set_child_ptr parent offset (Disk (id, cs))
               end )

  let create ?(at = In_memory) () =
    let from =
      match at with
      | In_memory -> `Load
      | Root _ -> `Root
      | At _ -> invalid_arg "Sector.create: At"
      | Freed -> invalid_arg "Sector.create: Freed"
    in
    let+ cstruct = B.allocate ~from () in
    let t =
      { id = at
      ; cstruct
      ; children = H.create 4
      ; parent = Detached
      ; depth = 0
      ; checksum = None
      }
    in
    B.set_finalize (Lru.value cstruct) (finalize_set_id t) ;
    t

  let drop_from_parent t =
    match t.parent with
    | Detached -> ()
    | Parent (parent, offset) ->
      assert (H.find parent.children offset == t) ;
      H.remove parent.children offset

  let drop_release t =
    assert (H.length t.children = 0) ;
    begin
      match t.id with
      | Root _ -> invalid_arg "Sector.drop_release: Root"
      | Freed -> invalid_arg "Sector.drop_release: Freed"
      | In_memory -> ()
      | At id -> B.discard id
    end ;
    B.unallocate t.cstruct ;
    t.id <- Freed ;
    drop_from_parent t

  let erase_region t ~off ~len =
    let* () = release t in
    let+ cstruct = rw_cstruct t in
    Cstruct.blit cstruct (off + len) cstruct off (Cstruct.length cstruct - off - len) ;
    for i = Cstruct.length cstruct - len to Cstruct.length cstruct - 1 do
      Cstruct.set_uint8 cstruct i 0
    done ;
    for i = off to off + len - 1 do
      match H.find_opt t.children i with
      | None -> ()
      | Some child ->
        assert (child.id = Freed) ;
        H.remove t.children i
    done ;
    for i = off + len to Cstruct.length cstruct - 1 do
      match H.find_opt t.children i with
      | None -> ()
      | Some child ->
        let j = i - len in
        assert (not (H.mem t.children j)) ;
        H.remove t.children i ;
        H.replace t.children j child ;
        child.parent <- Parent (t, j)
    done

  let length _t = B.page_size

  let get_uint8 t offset =
    let+ cstruct = ro_cstruct t in
    Cstruct.get_uint8 cstruct offset

  let get_uint16 t offset =
    let+ cstruct = ro_cstruct t in
    Cstruct.HE.get_uint16 cstruct offset

  let get_uint32 t offset =
    let+ cstruct = ro_cstruct t in
    Int32.to_int @@ Cstruct.HE.get_uint32 cstruct offset

  let get_uint64 t offset =
    let+ cstruct = ro_cstruct t in
    Cstruct.HE.get_uint64 cstruct offset

  let read_id t offset =
    let+ cstruct = ro_cstruct t in
    B.Id.read cstruct offset

  let set_uint8 t offset v =
    let* () = release t in
    let+ cstruct = rw_cstruct t in
    Cstruct.set_uint8 cstruct offset v

  let set_uint16 t offset v =
    let* () = release t in
    let+ cstruct = rw_cstruct t in
    Cstruct.HE.set_uint16 cstruct offset v

  let set_uint32 t offset v =
    let* () = release t in
    let+ cstruct = rw_cstruct t in
    Cstruct.HE.set_uint32 cstruct offset (Int32.of_int v)

  let set_uint64 t offset v =
    let* () = release t in
    let+ cstruct = rw_cstruct t in
    Cstruct.HE.set_uint64 cstruct offset v

  let write_id t offset v =
    let* () = release t in
    let+ cstruct = rw_cstruct t in
    B.Id.write cstruct offset v

  let get_child_ptr t offset =
    match H.find t.children offset with
    | v -> Lwt_result.return (Mem v)
    | exception Not_found ->
      let+ cstruct = ro_cstruct t in
      let id = B.Id.read cstruct offset in
      let cs = read_checksum cstruct offset in
      Disk (id, cs)

  let read ~from id =
    let* sector = B.allocate ~from () in
    let* cstruct = B.cstruct sector in
    let+ () = B.read id cstruct in
    sector

  let load_root ?(check = true) id =
    let* cstruct = read ~from:`Root id in
    let+ () = if check then check_root_checksum ~id cstruct else Lwt_result.return () in
    let t =
      { id = Root id
      ; cstruct
      ; children = H.create 4
      ; parent = Detached
      ; depth = 0
      ; checksum = None
      }
    in
    B.set_finalize (Lru.value cstruct) (fun _ -> failwith "finalize root") ;
    t

  let load = function
    | Disk (id, cs) ->
      let+ cstruct = read ~from:`Load id in
      let t =
        { id = At id
        ; cstruct
        ; children = H.create 4
        ; parent = Detached
        ; depth = 0
        ; checksum = Some cs
        }
      in
      B.set_finalize (Lru.value cstruct) (finalize_set_id t) ;
      t
    | Mem t -> Lwt_result.return t

  let get_child t offset =
    let* child = get_child_ptr t offset in
    match child with
    | Mem child -> Lwt_result.return child
    | Disk (id, cs) ->
      let+ cstruct = read ~from:`Load id in
      let child =
        { id = At id
        ; cstruct
        ; children = H.create 4
        ; parent = Parent (t, offset)
        ; depth = t.depth + 1
        ; checksum = Some cs
        }
      in
      B.set_finalize (Lru.value cstruct) (finalize_set_id child) ;
      assert (not (H.mem t.children offset)) ;
      H.add t.children offset child ;
      child

  let set_child t offset child =
    let* () = release t in
    set_child t offset child

  let rec count_new t acc =
    let* acc' = count_new_children t acc in
    match t.id with
    | Freed -> invalid_arg "Sector.count_new: freed"
    | Root _ | At _ ->
      if acc' = acc
      then Lwt_result.return acc'
      else begin
        let+ () = release t in
        acc' + 1
      end
    | In_memory -> Lwt_result.return (acc' + 1)

  and count_new_children t acc =
    let rec go acc = function
      | [] -> Lwt_result.return acc
      | x :: xs ->
        let* acc = count_new x acc in
        go acc xs
    in
    go acc @@ List.of_seq @@ H.to_seq_values t.children

  let count_new t = count_new t 0

  let rec lwt_result_fold f acc = function
    | [] -> Lwt_result.return acc
    | x :: xs ->
      let* acc = f acc x in
      lwt_result_fold f acc xs

  let rec finalize t ids acc =
    let* ids, acc = finalize_children t ids acc in
    let* cs =
      match t.checksum with
      | Some cs -> Lwt_result.return cs
      | None ->
        let+ cstruct = ro_cstruct t in
        let cs = get_checksum cstruct in
        t.checksum <- Some cs ;
        cs
    in
    begin
      match t.id, ids with
      | In_memory, id :: ids ->
        t.id <- At id ;
        let+ cstruct = ro_cstruct t in
        begin
          match t.parent with
          | Detached -> ()
          | Parent (parent, offset) ->
            assert (H.find parent.children offset == t) ;
            H.remove parent.children offset
        end ;
        B.set_id t.cstruct id ;
        id, cs, (id, cstruct) :: acc, ids
      | (Root id | At id), _ -> Lwt_result.return (id, cs, acc, ids)
      | Freed, _ -> invalid_arg "Sector.finalize: freed"
      | In_memory, [] -> invalid_arg "Sector.finalize: empty list"
    end

  and finalize_children t ids acc =
    let children =
      List.sort (fun (i, _) (j, _) -> Int.compare i j)
      @@ List.of_seq
      @@ H.to_seq t.children
    in
    let+ result =
      lwt_result_fold
        (fun (ids, acc) (offset, child) ->
          match child.id with
          | Freed -> failwith "Sector.finalize: freed children"
          | Root id | At id ->
            let prev_acc, prev_ids = acc, ids in
            let prev_child_cs =
              match child.checksum with
              | Some cs -> cs
              | None -> failwith "child has no checksum"
            in
            let+ child_id, child_cs, acc, ids = finalize child ids acc in
            assert (id = child_id) ;
            assert (acc == prev_acc) ;
            assert (ids == prev_ids) ;
            assert (C.equal child_cs prev_child_cs) ;
            B.set_id child.cstruct id ;
            H.remove t.children offset ;
            ids, acc
          | In_memory ->
            let* child_id, child_cs, acc, ids = finalize child ids acc in
            assert (At child_id = child.id) ;
            assert (Some child_cs = child.checksum) ;
            assert (not (H.mem t.children offset)) ;
            let* () = release t in
            let+ t_cstruct = rw_cstruct t in
            B.Id.write t_cstruct offset child_id ;
            set_checksum t_cstruct offset child_cs ;
            ids, acc)
        (ids, acc)
        children
    in
    assert (H.length t.children = 0) ;
    result

  let finalize t ids =
    let* e = count_new t in
    let+ _, _, ts, ids' = finalize t ids [] in
    assert (List.length ids' + e = List.length ids) ;
    ts, ids'

  let force_id t =
    match t.id with
    | At id | Root id -> id
    | In_memory -> invalid_arg "Sector.force_id: in memory"
    | Freed -> invalid_arg "Sector.force_id: freed"

  let blit_from_string str i t j n =
    let* () = release t in
    let+ cstruct = rw_cstruct t in
    Cstruct.blit_from_string str i cstruct j n

  let blit_to_bytes t i bytes j n =
    if n = 0
    then Lwt_result.return ()
    else
      let+ cstruct = ro_cstruct t in
      Cstruct.blit_to_bytes cstruct i bytes j n

  let to_write t =
    let+ cstruct = ro_cstruct t in
    force_id t, cstruct

  let write_root t =
    match t.id with
    | Root id ->
      let* cstruct = B.cstruct t.cstruct in
      let cs = compute_root_checksum cstruct in
      C.write cstruct root_checksum_offset cs ;
      B.write id [ cstruct ]
    | _ -> failwith "Sector.write_root: not a generation"
end

module Make (Clock : Mirage_clock.PCLOCK) (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Queue = Queue.Make (B)
  module Rope = Rope.Make (B)
  open Lwt_result.Syntax

  type key = string list
  type file = Rope.t ref
  type raw_time = float [@@deriving repr]

  type raw_fs =
    | Raw_dir of (string * raw_time * raw_fs) list
    | Raw_file of Sector.ptr
  [@@deriving repr]

  module M = Map.Make (String)

  type time = Ptime.t

  type fs =
    | Dir of time * fs M.t
    | File of time * Rope.t ref

  type t =
    { mutable on_disk : Rope.t
    ; files : fs M.t
    ; mutable dirty : bool
    }

  exception File_expected
  exception Dir_expected

  let string_of_raw = Repr.(unstage (to_bin_string raw_fs_t))
  let raw_of_string = Repr.(unstage (of_bin_string raw_fs_t))

  let count_new fs =
    let rec count_new _segment fs acc =
      match fs with
      | Dir (_, fs') -> M.fold count_new fs' acc
      | File (_, rope) ->
        let* acc = acc in
        let* c = Rope.count_new !rope in
        Lwt_result.return (acc + c)
    in
    M.fold count_new fs (Lwt_result.return 0)

  let fake fs =
    let rec fake segment fs acc =
      match fs with
      | Dir (_, fs') -> (segment, 0., Raw_dir (M.fold fake fs' [])) :: acc
      | File (_, _rope) -> (segment, 0., Raw_file Sector.null_ptr) :: acc
    in
    Raw_dir (M.fold fake fs [])

  let count_new t =
    let* files_count = count_new t.files in
    let+ on_disk_count =
      if files_count = 0 && (not t.dirty) && false
      then Lwt_result.return 0
      else begin
        let str = string_of_raw (fake t.files) in
        let* rope = Rope.of_string str in
        let* () = Rope.free t.on_disk in
        t.on_disk <- rope ;
        Rope.count_new rope
      end
    in
    files_count + on_disk_count

  let flush_ropes fs allocated =
    let rec flush_ropes segment fs acc =
      match fs with
      | Dir (time, fs') ->
        let* files, to_flush, allocated = acc in
        let+ files', to_flush', allocated' =
          M.fold flush_ropes fs' (Lwt_result.return ([], [], allocated))
        in
        ( (segment, Ptime.to_float_s time, Raw_dir files') :: files
        , List.rev_append to_flush' to_flush
        , allocated' )
      | File (time, rope) ->
        let* files, to_flush, allocated = acc in
        let* to_flush', allocated =
          if Sector.is_in_memory !rope
          then Sector.finalize !rope allocated
          else Lwt_result.return ([], allocated)
        in
        let ptr = Sector.to_ptr !rope in
        let acc =
          ( (segment, Ptime.to_float_s time, Raw_file ptr) :: files
          , List.rev_append to_flush' to_flush
          , allocated )
        in
        Lwt_result.return acc
    in
    let+ files, to_flush, allocated =
      M.fold flush_ropes fs (Lwt_result.return ([], [], allocated))
    in
    Raw_dir files, to_flush, allocated

  let to_payload t allocated =
    let* files, to_flush, allocated = flush_ropes t.files allocated in
    let str = string_of_raw files in
    let* on_disk_size = Rope.size t.on_disk in
    assert (String.length str = on_disk_size) ;
    let* rope = Rope.blit_from_string t.on_disk 0 str 0 (String.length str) in
    t.on_disk <- rope ;
    let+ to_flush', allocated = Sector.finalize t.on_disk allocated in
    assert (List.length to_flush' > 0) ;
    t.dirty <- false ;
    rope, List.rev_append to_flush' to_flush, allocated

  let of_raw raw =
    let rec of_raw acc = function
      | segment, time, Raw_dir lst ->
        let* acc = acc in
        let+ fs = List.fold_left of_raw (Lwt_result.return M.empty) lst in
        M.add segment (Dir (Option.get @@ Ptime.of_float_s time, fs)) acc
      | filename, time, Raw_file id ->
        let* acc = acc in
        let+ rope = Rope.load id in
        M.add filename (File (Option.get @@ Ptime.of_float_s time, ref rope)) acc
    in
    List.fold_left of_raw (Lwt_result.return M.empty) raw

  let of_disk_repr on_disk =
    let* str = Rope.to_string on_disk in
    let raw = raw_of_string str in
    match raw with
    | Ok (Raw_dir raw) ->
      let+ files = of_raw raw in
      files
    | Ok (Raw_file _) -> assert false (* root cannot be a file *)
    | Error (`Msg err) -> Lwt.fail_with err

  let make () =
    let+ on_disk = Rope.create () in
    { on_disk; files = M.empty; dirty = false }

  let load on_disk_ptr =
    if Sector.is_null_ptr on_disk_ptr
    then make ()
    else
      let* on_disk = Rope.load on_disk_ptr in
      let+ files = of_disk_repr on_disk in
      { on_disk; files; dirty = false }

  let verify_checksum t =
    let rec check = function
      | Dir (_, fs) ->
        let check = M.map check fs in
        M.fold (fun _ a b -> Lwt_result.bind a (fun () -> b)) check (Lwt_result.return ())
      | File (_, rope) -> Rope.verify_checksum !rope
    in
    let check = M.map check t.files in
    M.fold (fun _ a b -> Lwt_result.bind a (fun () -> b)) check (Lwt_result.return ())

  let exists t filename =
    let rec exists fs = function
      | [] -> Some `Dictionary
      | segment :: [] ->
        (match M.find_opt segment fs with
         | None -> None
         | Some (Dir _) -> Some `Dictionary
         | Some (File _) -> Some `Value)
      | segment :: xs ->
        (match M.find_opt segment fs with
         | None | Some (File _) -> None
         | Some (Dir (_, fs)) -> exists fs xs)
    in
    exists t.files filename

  let last_modified t filename =
    let rec last_modified fs = function
      | [] -> raise Not_found
      | segment :: [] ->
        (match M.find segment fs with
         | Dir (time, _) -> time
         | File (time, _) -> time)
      | segment :: xs ->
        (match M.find segment fs with
         | Dir (_, fs) -> last_modified fs xs
         | File _ -> raise Not_found)
    in
    last_modified t.files filename

  let find t filename =
    let rec find fs = function
      | [] -> raise Not_found
      | segment :: [] ->
        (match M.find segment fs with
         | Dir _ -> raise File_expected
         | File (_, r) -> r)
      | segment :: xs ->
        (match M.find segment fs with
         | Dir (_, fs) -> find fs xs
         | File _ -> raise Not_found)
    in
    find t.files filename

  let find_opt t filename =
    let rec find_opt fs = function
      | [] -> None
      | segment :: [] ->
        Option.bind (M.find_opt segment fs) (function
          | Dir _ -> None
          | File (_, r) -> Some r)
      | segment :: xs ->
        Option.bind (M.find_opt segment fs) (function
          | Dir (_, fs) -> find_opt fs xs
          | File _ -> None)
    in
    find_opt t.files filename

  let free fs =
    let rec free = function
      | Dir (_, fs) ->
        M.fold
          (fun _ fs acc ->
            let* () = acc in
            free fs)
          fs
          (Lwt_result.return ())
      | File (_, rope) -> Rope.free !rope
    in
    free (Dir (Ptime.epoch, fs))

  let add t filename rope =
    let time = Ptime.v @@ Clock.now_d_ps () in
    let rec create fs segment = function
      | [] -> M.add segment (File (time, rope)) fs
      | segment' :: xs ->
        let fs' = create M.empty segment' xs in
        M.add segment (Dir (time, fs')) fs
    in
    let rec add fs = function
      | [] -> assert false
      | segment :: [] ->
        (match M.find_opt segment fs with
         | None -> Lwt_result.return (M.add segment (File (time, rope)) fs)
         | Some (File (_, rope)) ->
           let+ () = Rope.free !rope in
           M.add segment (File (time, rope)) fs
         | Some (Dir (_, fs')) ->
           let+ () = free fs' in
           M.add segment (File (time, rope)) fs)
      | segment :: xs ->
        (match M.find_opt segment fs with
         | None -> Lwt_result.return (create fs segment xs)
         | Some (File (_, rope)) ->
           let+ () = Rope.free !rope in
           create fs segment xs
         | Some (Dir (_, fs')) ->
           let+ fs' = add fs' xs in
           M.add segment (Dir (time, fs')) fs)
    in
    let+ file = add t.files filename in
    t.dirty <- true ;
    { t with files = file }

  let remove t path =
    let time = Ptime.v @@ Clock.now_d_ps () in
    let rec remove fs = function
      | [] -> assert false
      | segment :: [] ->
        (match M.find_opt segment fs with
         | None -> Lwt_result.return fs
         | Some (Dir (_, fs')) ->
           let+ () = free fs' in
           M.remove segment fs
         | Some (File (_, rope)) ->
           let+ () = Rope.free !rope in
           M.remove segment fs)
      | segment :: xs ->
        (match M.find_opt segment fs with
         | None -> Lwt_result.return fs
         | Some (Dir (_, fs')) ->
           let+ fs' = remove fs' xs in
           M.add segment (Dir (time, fs')) fs
         | Some (File _) -> assert false)
    in
    t.dirty <- true ;
    let+ files = remove t.files path in
    { t with files }

  let rename t ~src ~dst =
    let time = Ptime.v @@ Clock.now_d_ps () in
    let rec find_and_remove_src fs = function
      | [] -> assert false (* cannot move root *)
      | segment :: [] ->
        let r = M.find segment fs in
        Lwt_result.return (r, M.remove segment fs)
      | segment :: xs ->
        (match M.find segment fs with
         | Dir (_, fs') ->
           let+ r, fs' = find_and_remove_src fs' xs in
           r, M.add segment (Dir (time, fs')) fs
         | File _ -> raise Not_found)
    in
    let rec create src_obj fs segment = function
      | [] -> M.add segment src_obj fs
      | segment' :: xs ->
        let fs' = create src_obj M.empty segment' xs in
        M.add segment (Dir (time, fs')) fs
    in
    let rec find_and_replace_dst src_obj fs = function
      | [] -> assert false (* cannot replace root *)
      | segment :: [] ->
        (match M.find_opt segment fs with
         | None -> Lwt_result.return (M.add segment src_obj fs)
         | Some (File (_, rope)) ->
           let+ () = Rope.free !rope in
           (match src_obj with
            | File (_, rope') ->
              rope := !rope' ;
              fs
            | Dir _ -> M.add segment src_obj fs)
         | Some (Dir (_, fs')) ->
           let+ () = free fs' in
           M.add segment src_obj fs)
      | segment :: xs ->
        (match M.find_opt segment fs with
         | None -> Lwt_result.return (create src_obj fs segment xs)
         | Some (File (_, rope)) ->
           let+ () = Rope.free !rope in
           create src_obj fs segment xs
         | Some (Dir (_, fs')) ->
           let+ fs' = find_and_replace_dst src_obj fs' xs in
           M.add segment (Dir (time, fs')) fs)
    in
    t.dirty <- true ;
    let* src_obj, files = find_and_remove_src t.files src in
    let+ files = find_and_replace_dst src_obj files dst in
    { t with files }

  let filename key = String.concat "/" key
  let size rope = Rope.size !rope
  let blit_to_bytes rope ~off ~len bytes = Rope.blit_to_bytes !rope off bytes 0 len

  let blit_from_string rope ~off ~len str =
    let+ t = Rope.blit_from_string !rope off str 0 len in
    rope := t

  let append_from rope arg =
    let+ t_rope = Rope.append_from !rope arg in
    rope := t_rope

  let list t path =
    let to_v = function
      | s, Dir _ -> s, `Dictionary
      | s, File _ -> s, `Value
    in
    let rec list fs = function
      | [] -> List.map to_v @@ M.bindings fs
      | segment :: xs ->
        (match M.find segment fs with
         | Dir (_, fs) -> list fs xs
         | File _ -> raise Dir_expected)
    in
    list t.files path

  (* let prefix' = if prefix = "/" then "/" else prefix ^ "/" in
    let prefix_len = String.length prefix' in
    let lst =
      let lst =
        List.filter_map (fun (filename, _) ->
          if not (String.starts_with ~prefix:prefix' filename)
          then None
          else (
            match String.index_from_opt filename prefix_len '/' with
            | None ->
              let len = String.length filename in
              let filename = String.sub filename prefix_len (len - prefix_len) in
              Some (filename, `Value)
            | Some offset ->
              let dirname = String.sub filename prefix_len (offset - prefix_len) in
              Some (dirname, `Dictionary)))
        @@ M.bindings t.files
      in
      List.sort_uniq Stdlib.compare lst
    in
    lst *)

  let reachable_size _t = failwith "to fix"
  (* if M.cardinal t.files = 0
    then Lwt_result.return 0
    else begin
      let* repr = Rope.reachable_size t.on_disk in
      let rec go acc = function
        | [] -> Lwt_result.return acc
        | (_, rope) :: rest ->
          let* s = Rope.reachable_size !rope in
          go (acc + s) rest
      in
      go repr @@ M.bindings t.files
    end *)

  let touch t filename str =
    let* rope = Rope.of_string str in
    let file = ref rope in
    let+ files = add t filename file in
    files, file
end

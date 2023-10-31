module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Queue = Queue.Make (B)
  module Rope = Rope.Make (B)
  open Lwt_result.Syntax

  type filename = string [@@deriving repr]
  type file = string * Rope.t ref
  type raw = (filename * Sector.ptr) list [@@deriving repr]

  module M = Map.Make (String)

  type t =
    { mutable on_disk : Rope.t
    ; files : Rope.t ref M.t
    ; mutable dirty : bool
    }

  let string_of_raw = Repr.(unstage (to_bin_string raw_t))
  let raw_of_string = Repr.(unstage (of_bin_string raw_t))

  let rec count_new_list acc = function
    | [] -> Lwt_result.return acc
    | (_filename, rope) :: xs ->
      let* c = Rope.count_new !rope in
      count_new_list (acc + c) xs

  let count_new t =
    let* files_count = count_new_list 0 @@ M.bindings t.files in
    let+ on_disk_count =
      if files_count = 0 && (not t.dirty) && (not !B.dirty) && false
      then Lwt_result.return 0
      else begin
        let fake =
          List.map (fun (filename, _) -> filename, Sector.null_ptr) (M.bindings t.files)
        in
        let str = string_of_raw fake in
        let* rope = Rope.of_string str in
        let* () = Rope.free t.on_disk in
        t.on_disk <- rope ;
        Rope.count_new rope
      end
    in
    files_count + on_disk_count

  let rec flush_ropes (files, to_flush, allocated) = function
    | [] -> Lwt_result.return (List.rev files, to_flush, allocated)
    | (filename, rope) :: rest ->
      let* to_flush', allocated =
        if Sector.is_in_memory !rope
        then Sector.finalize !rope allocated
        else Lwt_result.return ([], allocated)
      in
      let ptr = Sector.to_ptr !rope in
      let acc = (filename, ptr) :: files, List.rev_append to_flush' to_flush, allocated in
      flush_ropes acc rest

  let to_payload t allocated =
    let* files, to_flush, allocated =
      flush_ropes ([], [], allocated) (M.bindings t.files)
    in
    let str = string_of_raw files in
    let* on_disk_size = Rope.size t.on_disk in
    assert (String.length str = on_disk_size) ;
    let* rope = Rope.blit_from_string t.on_disk 0 str 0 (String.length str) in
    t.on_disk <- rope ;
    let+ to_flush', allocated = Sector.finalize t.on_disk allocated in
    assert (List.length to_flush' > 0) ;
    t.dirty <- false ;
    rope, List.rev_append to_flush' to_flush, allocated

  let rec of_raw acc = function
    | [] -> Lwt_result.return acc
    | (filename, id) :: lst ->
      let* rope = Rope.load id in
      let acc = M.add filename (ref rope) acc in
      of_raw acc lst

  let of_raw lst = of_raw M.empty lst

  let of_disk_repr on_disk =
    let* str = Rope.to_string on_disk in
    let raw = raw_of_string str in
    match raw with
    | Ok raw ->
      let+ files = of_raw raw in
      files
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
    let seq = M.to_seq t.files in
    let seq = Seq.map (fun (_, rope) () -> Rope.verify_checksum !rope) seq in
    Seq.fold_left Lwt_result.bind (Lwt_result.return ()) seq

  let mem t filename = M.mem filename t.files
  let find t filename = M.find filename t.files
  let find_opt t filename = M.find_opt filename t.files
  let add t filename rope = { t with files = M.add filename rope t.files }

  let remove t filename =
    match find_opt t filename with
    | None -> Lwt_result.return t
    | Some rope ->
      let+ () = Rope.free !rope in
      t.dirty <- true ;
      let files = M.remove filename t.files in
      { t with files }

  let rename t ~src ~dst =
    let src_file = find t src in
    let+ () =
      match find_opt t dst with
      | None -> Lwt_result.return ()
      | Some dst_file ->
        assert (dst_file != src_file) ;
        let+ () = Rope.free !dst_file in
        dst_file := !src_file
    in
    let files = M.remove src t.files in
    let files = M.add dst src_file files in
    t.dirty <- true ;
    { t with files }

  let size (_, rope) = Rope.size !rope

  let blit_to_bytes (_filename, rope) ~off ~len bytes =
    Rope.blit_to_bytes !rope off bytes 0 len

  let blit_from_string _t (_filename, rope) ~off ~len str =
    let+ t = Rope.blit_from_string !rope off str 0 len in
    rope := t

  let list t prefix =
    let prefix' = if prefix = "/" then "/" else prefix ^ "/" in
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
    lst

  let reachable_size t =
    if M.cardinal t.files = 0
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
    end
end

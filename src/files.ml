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

  let count_new t =
    let files_count = M.fold (fun _ s acc -> acc + Rope.count_new !s) t.files 0 in
    let+ on_disk_count =
      if files_count = 0 && not t.dirty
      then Lwt_result.return 0
      else begin
        let fake =
          List.map (fun (filename, _) -> filename, Sector.null_ptr) (M.bindings t.files)
        in
        let str = string_of_raw fake in
        let* rope = Rope.of_string str in
        let+ () = Rope.free t.on_disk in
        t.on_disk <- rope ;
        Rope.count_new rope
      end
    in
    files_count + on_disk_count

  let to_payload t allocated =
    let files, to_flush, allocated =
      List.fold_left
        (fun (files, to_flush, allocated) (filename, rope) ->
          let to_flush', allocated =
            if Sector.is_in_memory !rope
            then Sector.finalize !rope allocated
            else [], allocated
          in
          let ptr = Sector.to_ptr !rope in
          (filename, ptr) :: files, List.rev_append to_flush' to_flush, allocated)
        ([], [], allocated)
        (M.bindings t.files)
    in
    let str = string_of_raw files in
    assert (String.length str = Rope.size t.on_disk) ;
    let+ rope = Rope.blit_from_string t.on_disk 0 str 0 (String.length str) in
    t.on_disk <- rope ;
    let to_flush', allocated = Sector.finalize t.on_disk allocated in
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

  let make () = { on_disk = Rope.create (); files = M.empty; dirty = false }

  let load on_disk_ptr =
    if Sector.is_null_ptr on_disk_ptr
    then Lwt_result.return (make ())
    else
      let* on_disk = Rope.load on_disk_ptr in
      let+ files = of_disk_repr on_disk in
      { on_disk; files; dirty = false }

  let mem t filename = M.mem filename t.files
  let find t filename = M.find filename t.files
  let find_opt t filename = M.find_opt filename t.files
  let add t filename rope = { t with files = M.add filename rope t.files }

  let remove t filename =
    match find_opt t filename with
    | None -> Lwt_result.return t
    | Some rope ->
      let* r = Rope.of_string "" in
      let+ () = Rope.free !rope in
      rope := r ;
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
end

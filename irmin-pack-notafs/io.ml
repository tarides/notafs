module Int63 = Optint.Int63

module type S = Irmin_pack_io.Io_s

module Make (Clock : Mirage_clock.MCLOCK) (B : Mirage_block.S) = struct
  module Index_platform = Index_notafs.Make (Clock) (B)
  module Fs = Index_platform.Fs

  let init b = Index_platform.IO.init b
  let fs () = Index_platform.IO.fs ()
  let notafs_flush () = Lwt_direct.direct (fun () -> Fs.flush (fs ()))

  type t = Index_platform.IO.t
  type misc_error = unit [@@deriving irmin]

  type create_error =
    [ `Io_misc of misc_error
    | `File_exists of string
    ]

  type open_error =
    [ `Io_misc of misc_error
    | `No_such_file_or_directory of string
    | `Not_a_file
    ]

  type read_error =
    [ `Io_misc of misc_error
    | `Read_out_of_bounds
    | `Closed
    | `Invalid_argument
    ]

  type write_error =
    [ `Io_misc of misc_error
    | `Ro_not_allowed
    | `Closed
    ]

  type close_error =
    [ `Io_misc of misc_error
    | `Double_close
    ]

  type mkdir_error =
    [ `Io_misc of misc_error
    | `File_exists of string
    | `No_such_file_or_directory of string
    | `Invalid_parent_directory
    ]

  let create ~path ~overwrite =
    Lwt_direct.direct
    @@ fun () ->
    let open Lwt.Syntax in
    let t = fs () in
    if overwrite
    then
      let* () =
        if Fs.exists t path = Some `Value then Fs.remove t path else Lwt.return_unit
      in
      let+ r = Fs.touch t path "" in
      Ok r
    else (
      match Fs.find t path with
      | None ->
        let+ r = Fs.touch t path "" in
        Ok r
      | Some file -> Lwt.return (Ok file))

  let open_ ~path ~readonly:_ =
    match Fs.find (fs ()) path with
    | None -> Error (`No_such_file_or_directory path)
    | Some file -> Ok file

  let close _t =
    notafs_flush () ;
    Ok ()

  let write_string t ~off str =
    let off = Int63.to_int off in
    Ok
      (Lwt_direct.direct
       @@ fun () -> Fs.blit_from_string t ~off ~len:(String.length str) str)

  let fsync _t =
    notafs_flush () ;
    Ok ()

  let move_file ~src ~dst = Ok (Lwt_direct.direct @@ fun () -> Fs.rename (fs ()) ~src ~dst)

  let copy_file ~src ~dst =
    let _ = failwith (Printf.sprintf "Io.copy_file ~src:%S ~dst:%S" src dst) in
    Error (`Sys_error "copy_file")

  let readdir _path = []
  let rmdir _path = ()
  let mkdir _path = Ok ()
  let unlink path = Ok (Lwt_direct.direct @@ fun () -> Fs.remove (fs ()) path)

  let unlink_dont_wait ~on_exn:_ path =
    Lwt_direct.direct @@ fun () -> Fs.remove (fs ()) path

  let read_to_string t ~off ~len =
    Lwt_direct.direct
    @@ fun () ->
    let bytes = Bytes.create len in
    let open Lwt.Syntax in
    let off = Int63.to_int off in
    let+ _ = Fs.blit_to_bytes t ~off ~len bytes in
    Ok (Bytes.to_string bytes)

  let read_all_to_string t =
    Lwt_direct.direct
    @@ fun () ->
    let open Lwt.Syntax in
    let* len = Fs.size t in
    let bytes = Bytes.create len in
    let+ _ = Fs.blit_to_bytes t ~off:0 ~len bytes in
    Ok (Bytes.to_string bytes)

  let read_size t =
    Lwt_direct.direct
    @@ fun () ->
    let open Lwt.Syntax in
    let+ len = Fs.size t in
    Ok (Int63.of_int len)

  let size_of_path path =
    match Fs.find (fs ()) path with
    | None -> Error (`No_such_file_or_directory path)
    | Some file -> read_size file

  let classify_path path =
    match Fs.exists (fs ()) path with
    | Some `Value -> `File
    | Some `Dictionary -> `Directory
    | None -> `No_such_file_or_directory

  let readonly _t = false
  let path t = Fs.filename t
  let page_size = -1

  let read_exn t ~off ~len bytes =
    let off = Int63.to_int off in
    let q = Lwt_direct.direct @@ fun () -> Fs.blit_to_bytes t ~off ~len bytes in
    assert (q = len)

  let write_exn t ~off ~len str =
    let off = Int63.to_int off in
    Lwt_direct.direct @@ fun () -> Fs.blit_from_string t ~off ~len str

  exception Misc of misc_error

  let raise_misc_error misc_error = raise (Misc misc_error)

  let catch_misc_error fn =
    try Ok (fn ()) with
    | Misc m -> Error (`Io_misc m)

  module Stats = struct
    let get_wtime () = 0.0
    let get_stime () = 0.0
    let get_utime () = 0.0

    let get_rusage () =
      Irmin_pack_io.Stats_intf.Latest_gc.
        { maxrss = Int64.zero
        ; minflt = Int64.zero
        ; majflt = Int64.zero
        ; inblock = Int64.zero
        ; oublock = Int64.zero
        ; nvcsw = Int64.zero
        ; nivcsw = Int64.zero
        }
  end

  module Clock = struct
    type counter = int64

    let counter () = Clock.elapsed_ns ()

    let count t =
      let now = Clock.elapsed_ns () in
      Mtime.Span.of_uint64_ns (Int64.sub now t)

    let start = counter ()
    let elapsed () = count start

    let now () =
      let now = Clock.elapsed_ns () in
      Mtime.of_uint64_ns (Int64.sub now start)
  end

  module Progress = Progress_engine.Make (struct
      module Clock = Clock

      module Terminal_width = struct
        let set_changed_callback _ = ()
        let get () = None
      end
    end)
end

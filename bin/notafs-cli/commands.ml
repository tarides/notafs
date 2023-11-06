open Lwt_result.Syntax
open Context

let on_error s = function
  | Ok () -> ()
  | Error err -> Fmt.pr "Encountered error in %S: %a@." s Disk.pp_error err

let format path page_size =
  let format () =
    let* block =
      Lwt_result.ok (Block.connect ~prefered_sector_size:(Some page_size) path)
    in
    let+ _disk = Disk.format block in
    Fmt.pr "Disk at %S succesfully formatted@." path
  in
  on_error "format" @@ Lwt_main.run (format ())

let string_of_disk_space space =
  let kb = 1024L in
  let mb = Int64.mul 1024L kb in
  let gb = Int64.mul 1024L mb in
  if space < kb
  then Int64.to_string space ^ "b"
  else if space < mb
  then Int64.to_string (Int64.div space kb) ^ "kb"
  else if space < gb
  then Int64.to_string (Int64.div space mb) ^ "mb"
  else Int64.to_string (Int64.div space gb) ^ "gb"

let info_cmd disk =
  let infos () =
    let page_size = Int64.of_int (Disk.page_size disk) in
    let disk_space = Int64.mul (Disk.disk_space disk) page_size in
    let free_space = Int64.mul (Disk.free_space disk) page_size in
    Fmt.pr "Disk space: %s@." (string_of_disk_space disk_space) ;
    Fmt.pr "Free space: %s@." (string_of_disk_space free_space) ;
    Fmt.pr "Sector size: %s@." (string_of_disk_space page_size) ;
    Lwt_result.return ()
  in
  on_error "info" @@ Lwt_main.run (infos ())

let touch disk path =
  let touch () =
    let k = Mirage_kv.Key.v path in
    let+ () = Disk.set disk k "" in
    Fmt.pr "File %S created@." path
  in
  on_error "touch" @@ Lwt_main.run (touch ())

let remove disk path =
  let remove () =
    let k = Mirage_kv.Key.v path in
    let+ () = Disk.remove disk k in
    Fmt.pr "File %S removed@." path
  in
  on_error "remove" @@ Lwt_main.run (remove ())

let rename disk path_from path_to =
  let rename () =
    let source = Mirage_kv.Key.v path_from in
    let dest = Mirage_kv.Key.v path_to in
    let+ () = Disk.rename disk ~source ~dest in
    Fmt.pr "File %S renamed to %S@." path_from path_to
  in
  on_error "rename" @@ Lwt_main.run (rename ())

let cat disk path =
  let cat () =
    let key = Mirage_kv.Key.v path in
    let+ get = Disk.get disk key in
    Fmt.pr "%s" get
  in
  on_error "cat" @@ Lwt_main.run (cat ())

let copy_to_disk disk path_from path_to =
  let key = Mirage_kv.Key.v path_to in
  let size = (Unix.stat path_from).st_size in
  let bytes = Bytes.create size in
  let fd = Unix.openfile path_from Unix.[ O_RDONLY ] 0o0 in
  let rec fill_bytes off rem_size =
    let read = Unix.read fd bytes off rem_size in
    if read < rem_size then fill_bytes (off + read) (rem_size - read)
  in
  fill_bytes 0 size ;
  let+ () = Disk.set disk key (Bytes.to_string bytes) in
  Fmt.pr "File %S has been copied to key %S@." path_from path_to ;
  Unix.close fd

let copy_from_disk disk path_from path_to =
  let key = Mirage_kv.Key.v path_from in
  let+ get = Disk.get disk key in
  let fd = Unix.openfile path_to Unix.[ O_WRONLY; O_CREAT; O_TRUNC ] 0o664 in
  let size = String.length get in
  let bytes = String.to_bytes get in
  let rec dump_bytes off rem_size =
    let write = Unix.write fd bytes off rem_size in
    if write < rem_size then dump_bytes (off + write) (rem_size - write)
  in
  dump_bytes 0 size ;
  Fmt.pr "Key %S has been copied to file %S@." path_from path_to ;
  Unix.close fd

let copy_from_disk_to_disk disk path_from path_to =
  let key_from = Mirage_kv.Key.v path_from in
  let key_to = Mirage_kv.Key.v path_to in
  let* from = Disk.get disk key_from in
  let+ () = Disk.set disk key_to from in
  Fmt.pr "Key %S has been copied to key %S@." path_from path_to

let copy disk path_from path_to =
  let copy () =
    let disk_id = "@" in
    let disk_id_len = String.length disk_id in
    match
      ( String.starts_with ~prefix:disk_id path_from
      , String.starts_with ~prefix:disk_id path_to )
    with
    | true, true ->
      copy_from_disk_to_disk
        disk
        (String.sub path_from disk_id_len (String.length path_from - disk_id_len))
        (String.sub path_to disk_id_len (String.length path_to - disk_id_len))
    | true, false ->
      copy_from_disk
        disk
        (String.sub path_from disk_id_len (String.length path_from - disk_id_len))
        path_to
    | false, true ->
      copy_to_disk
        disk
        path_from
        (String.sub path_to disk_id_len (String.length path_to - disk_id_len))
    | false, false ->
      Lwt_result.fail
        (`Unsupported_operation
          (Fmt.str "No disk paths (prefix disk paths with %S)@." disk_id))
  in
  on_error "copy" @@ Lwt_main.run (copy ())

let stats disk path =
  let cat () =
    let key = Mirage_kv.Key.v path in
    (* let* last_modified = Disk.last_modified disk key in *)
    let+ size = Disk.size disk key in
    Fmt.pr "Size: %a@." Optint.Int63.pp size ;
    Fmt.pr "Last modified: %s@." "<Not Supported Yet>"
  in
  on_error "stats" @@ Lwt_main.run (cat ())

let list disk path =
  let list () =
    let open Lwt_result.Syntax in
    let k = Mirage_kv.Key.v path in
    let+ files = Disk.list disk k in
    let styled t pp =
      match t with
      | `Value -> pp
      | `Dictionary -> Fmt.styled `Bold (Fmt.styled (`Fg `Blue) pp)
    in
    List.iter (fun (key, t) -> Fmt.pr "%a@." (styled t Mirage_kv.Key.pp) key) files
  in
  on_error "list" @@ Lwt_main.run (list ())

open Cmdliner
(** Commands *)

open Common_args

(* Format *)
let page_size =
  Arg.(
    value
    & opt int 512
    & info [ "p"; "page_size" ] ~docv:"page_size" ~doc:"size of the disk's page")

let format_cmd =
  let doc = "formats a disk for further use" in
  let info = Cmd.info "format" ~doc in
  Cmd.v info Term.(const format $ disk_path $ page_size)

(* Info *)
let info_cmd =
  let doc = "show available disk space" in
  let info = Cmd.info "info" ~doc in
  Cmd.v info Term.(const info_cmd $ disk)

(* Touch *)
let touch_cmd =
  let doc = "create a file in a formatted disk" in
  let info = Cmd.info "touch" ~doc in
  Cmd.v info Term.(const touch $ disk $ file_path)

(* Remove *)
let remove_cmd =
  let doc = "remove a file from a formatted disk" in
  let info = Cmd.info "remove" ~doc in
  Cmd.v info Term.(const remove $ disk $ file_path)

(* Rename *)
let old_path =
  Arg.(
    required
    & pos 0 (some string) None
    & info [] ~docv:"OLD_PATH" ~doc:"path to rename from")

let new_path =
  Arg.(
    required
    & pos 1 (some string) None
    & info [] ~docv:"NEW_PATH" ~doc:"path to rename to")

let rename_cmd =
  let doc = "rename a file in a formatted disk" in
  let info = Cmd.info "rename" ~doc in
  Cmd.v info Term.(const rename $ disk $ old_path $ new_path)

(* Cat *)
let cat_cmd =
  let doc = "dump a file from a formatted disk" in
  let info = Cmd.info "cat" ~doc in
  Cmd.v info Term.(const cat $ disk $ file_path)

(* Stats *)
let stats_cmd =
  let doc = "gives some stats about a file from a formatted disk" in
  let info = Cmd.info "stats" ~doc in
  Cmd.v info Term.(const stats $ disk $ file_path)

(* Copy *)
let file_path n =
  Arg.(
    required
    & pos n (some string) None
    & info
        []
        ~docv:"FILE_PATH"
        ~doc:"path to copy from/to (prefix with '@' for disk paths)")

let copy_cmd =
  let doc = "copies a file from/to a formatted disk, disk paths need the prefix '@'" in
  let info = Cmd.info "copy" ~doc in
  Cmd.v info Term.(const copy $ disk $ file_path 0 $ file_path 1)

(* List *)
let path =
  Arg.(value & pos ~rev:true 0 string "/" & info [] ~docv:"PATH" ~doc:"path to list")

let list_cmd =
  let doc = "lists the files available on a disk" in
  let info = Cmd.info "list" ~doc in
  Cmd.v info Term.(const list $ disk $ path)

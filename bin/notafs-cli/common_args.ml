open Cmdliner
open Context

let return_disk = function
  | Error err ->
    Fmt.pr "Disk connect failed: %a@." Disk.pp_error err ;
    exit 1
  | Ok disk -> Lwt.return disk

let connect_block disk_path = Lwt_main.run (Block.connect disk_path)

let connect_disk disk_path =
  let make_disk =
    let open Lwt.Syntax in
    let* block = Block.connect disk_path in
    let disk = Disk.connect block in
    Lwt.bind disk (function
      | Error (`Wrong_page_size page_size) ->
        let* () = Block.disconnect block in
        let* block = Block.connect ~prefered_sector_size:(Some page_size) disk_path in
        let disk = Disk.connect block in
        Lwt.bind disk return_disk
      | r -> return_disk r)
  in
  Lwt_main.run make_disk

(* Disk *)

let disk_path_flag =
  Arg.(
    required
    & opt (some file) None
    & info [ "d"; "disk" ] ~docv:"DISK_PATH" ~doc:"path to a disk")

let disk_path_pos =
  Arg.(required & pos 0 (some file) None & info [] ~doc:"path to a disk")

let disk = Term.(const connect_disk $ disk_path_flag)
let block = Term.(const connect_block $ disk_path_pos)

let file_path =
  Arg.(
    required
    & pos ~rev:true 0 (some string) None
    & info [] ~docv:"FILE_PATH" ~doc:"path to a file")

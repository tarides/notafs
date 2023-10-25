open Cmdliner
open Context

let connect_disk disk_path =
  let make_disk =
    let open Lwt.Syntax in
    let* block = Block.connect disk_path in
    let disk = Disk.connect block in
    Lwt.bind disk (function
      | Error err ->
        Fmt.pr "Encountered error: %a@." Disk.pp_error err ;
        exit 1
      | Ok disk -> Lwt.return disk)
  in
  Lwt_main.run make_disk

(* Disk *)
let disk_path =
  Arg.(
    required
    & opt (some file) None
    & info [ "d"; "disk" ] ~docv:"DISK_PATH" ~doc:"path to a disk")

let disk = Term.(const connect_disk $ disk_path)

let file_path =
  Arg.(
    required
    & pos ~rev:true 0 (some string) None
    & info [] ~docv:"FILE_PATH" ~doc:"path to a file")

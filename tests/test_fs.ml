open Cmdliner
module Disk = Notafs.KV (Pclock) (Notafs.Adler32) (Block)

let on_error s = function
  | Ok () -> ()
  | Error err -> Fmt.pr "Encountered error in %S: %a@." s Disk.pp_error err

let prefered_sector_size = Some 1024

let rec make_tree n acc =
  match n with
  | 0 ->
    let value = String.concat "/" (List.rev acc) in
    let key = Mirage_kv.Key.v value in
    [ key, value ]
  | n ->
    let l =
      List.init n (fun i ->
        let r = Random.int n in
        make_tree r (Fmt.str "%d" i :: acc))
    in
    List.flatten l

let make_tree n = make_tree n []

let fs_tests disk =
  let tree = make_tree 7 in
  Fmt.pr "Tree:@.%a@." Fmt.Dump.(list @@ pair Mirage_kv.Key.pp string) tree ;
  let r = List.map (fun (k, v) -> Disk.set disk k v) tree in
  List.fold_left (fun a b -> Lwt_result.bind a (fun () -> b)) (Lwt_result.return ()) r

let main disk_path =
  let open Lwt_result.Syntax in
  let* block = Lwt_result.ok (Block.connect ~prefered_sector_size disk_path) in
  let* disk = Disk.format block in
  let* () = fs_tests disk in
  let* _disk = Disk.connect block in
  let* () = Lwt_result.ok (Disk.disconnect disk) in
  Lwt_result.ok (Block.disconnect block)

let main disk_path =
  Random.self_init () ;
  on_error "test_fs" @@ Lwt_main.run (main disk_path)

(* Disk *)
let disk_path =
  Arg.(
    required
    & opt (some file) None
    & info [ "d"; "disk" ] ~docv:"DISK_PATH" ~doc:"path to a disk")

let main_cmd =
  let info = Cmd.info "graphics" in
  Cmd.v info Term.(const main $ disk_path)

let () = exit (Cmd.eval ~catch:false main_cmd)

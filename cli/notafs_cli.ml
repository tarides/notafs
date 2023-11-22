open Cmdliner
open Commands

let setup () =
  let style_renderer = `Ansi_tty in
  Fmt_tty.setup_std_outputs ~style_renderer ()

(* Main command *)
let main_cmd =
  let doc = "cli for notafs disks" in
  let info = Cmd.info "notafs-cli" ~version:"%%VERSION%%" ~doc in
  let default = Term.(ret (const (`Help (`Pager, None)))) in
  let commands =
    Cmd.group
      info
      ~default
      [ format_cmd
      ; info_cmd
      ; touch_cmd
      ; remove_cmd
      ; rename_cmd
      ; cat_cmd
      ; copy_cmd
      ; stats_cmd
      ; list_cmd
      ; tree_cmd
      ]
  in
  commands

let () =
  setup () ;
  exit (Cmd.eval ~catch:false main_cmd)

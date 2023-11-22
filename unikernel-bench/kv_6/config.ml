open Mirage

let remote = "relativize://docteur"

let main =
  main
    "Unikernel.Main"
    (pclock @-> block @-> kv_ro @-> job)
    ~packages:[ package "notafs"; package "tar-mirage" ]

let img = if_impl Key.is_solo5 (block_of_file "storage") (block_of_file "/tmp/storage")

let () =
  register
    "block_test"
    [ main $ default_posix_clock $ img $ docteur remote ~mode:`Fast ~branch:"refs/heads/master" ]

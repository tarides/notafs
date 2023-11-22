open Mirage

let remote = "relativize://docteur"

let main =
  main
    "Unikernel.Main"
    (block @-> kv_ro @-> job)
    ~packages:[ package "notafs"; package "tar-mirage" ]

let img = if_impl Key.is_solo5 (block_of_file "storage") (block_of_file "/tmp/storage")

let () =
  register
    "block_test"
    [ main $ img $ docteur remote ~mode:`Fast ~branch:"refs/heads/master" ]

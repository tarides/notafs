open Mirage

let main =
  main
    "Unikernel.Main"
    (block @-> job)
    ~packages:[ package "eio" ~sublibs:[ "mock" ]; package "irmin-pack-notafs" ]

let img = if_impl Key.is_solo5 (block_of_file "storage") (block_of_file "/tmp/storage")
let () = register "block_test" [ main $ img ]

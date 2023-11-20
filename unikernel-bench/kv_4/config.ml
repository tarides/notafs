open Mirage

(*let remote = "file://home/cha/Documents/github/unikernel-kv/monrepo"*)

let main =
  main
    "Unikernel.Main"
    (block @-> job)
    ~packages:
      [ package "mirage-kv"
      ; package "fat-filesystem"
      ; package "chamelon" ~min:"0.1.1" ~sublibs:[ "kv" ]
      ]

let img = if_impl Key.is_solo5 (block_of_file "storage") (block_of_file "/tmp/storage")
let () = register "block_test" [ main $ img ]

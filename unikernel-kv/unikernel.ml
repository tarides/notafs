open Lwt.Syntax

module Main (Block : Mirage_block.S) = struct
  module Kv = Notafs.KV (Pclock) (Notafs.Adler32) (Block)

  let force lwt =
    let open Lwt.Infix in
    lwt
    >|= function
    | Ok v -> v
    | Error e ->
      Format.printf "ERROR: %a@." Kv.pp_error e ;
      failwith "error"

  let start block =
    let* fs = Kv.connect block in
    let* fs =
      match fs with
      | Ok fs -> Lwt.return fs
      | Error `Disk_not_formatted ->
        let* fs = force @@ Kv.format block in
        let+ () = force @@ Kv.set fs (Mirage_kv.Key.v "hello") "world!" in
        fs
      | Error e ->
        Format.printf "ERROR: %a@." Kv.pp_error e ;
        failwith "unexpected error"
    in
    let* contents = force @@ Kv.get fs (Mirage_kv.Key.v "hello") in
    Format.printf "%S@." contents ;
    let* () = Block.disconnect block in
    Lwt.return_unit
end

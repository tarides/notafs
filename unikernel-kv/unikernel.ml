open Lwt.Syntax

module Main (Pclock : Mirage_clock.PCLOCK) (Block : Mirage_block.S) = struct
  module Kv = Notafs.KV (Pclock) (Notafs.Adler32) (Block)

  let force lwt =
    let open Lwt.Infix in
    lwt
    >|= function
    | Ok v -> v
    | Error _ -> failwith "error"

  let start _pclock block =
    let* fs = force @@ Kv.format block in
    let* () = force @@ Kv.set fs (Mirage_kv.Key.v "hello") "world!" in
    let* contents = force @@ Kv.get fs (Mirage_kv.Key.v "hello") in
    Format.printf "%S@." contents ;
    let* () = Block.disconnect block in
    Lwt.return_unit
end

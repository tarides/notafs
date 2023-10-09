open Lwt.Syntax

module Main (Block : Mirage_block.S) = struct
  module Block = struct
    include Block

    let discard _ _ = ()
    let flush _ = ()
  end

  module Kv = Notafs.KV (Block)

  let force lwt =
    let open Lwt.Infix in
    lwt
    >|= function
    | Ok v -> v
    | Error _ -> failwith "error"

  let start block =
    let* fs = force @@ Kv.format block in
    let* () = force @@ Kv.set fs (Mirage_kv.Key.v "hello") "world!" in
    let* contents = force @@ Kv.get fs (Mirage_kv.Key.v "hello") in
    Format.printf "%S@." contents ;
    let* () = Block.disconnect block in
    Lwt.return_unit
end

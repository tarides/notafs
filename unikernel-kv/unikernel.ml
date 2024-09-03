open Lwt.Syntax

module Main (KV : Mirage_kv.RW) = struct
  let force lwt =
    let open Lwt.Infix in
    lwt >|= function
    | Ok v -> v
    | Error e ->
        Logs.err (fun f -> f "Error: %a" KV.pp_write_error e);
        failwith "fatal error"

  let start kv =
    let key = Mirage_kv.Key.v "hello" in
    let* result = KV.get kv key in
    let* () =
      match result with
      | Ok contents ->
          Logs.info (fun f -> f "Key hello contains %S" contents);
          Lwt.return_unit
      | Error _ ->
          Logs.warn (fun f -> f "Key hello doesn't exist, creating it!");
          force @@ KV.set kv key "world!"
    in
    let* () = KV.disconnect kv in
    Lwt.return_unit
end

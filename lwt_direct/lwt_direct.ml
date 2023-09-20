open Effect.Deep

type _ Effect.t += Lwt : (unit -> 'a Lwt.t) -> 'a Effect.t

let direct lwt = Effect.perform (Lwt lwt)

let indirect fn =
  match_with
    fn
    ()
    { retc = Lwt.return
    ; exnc = (fun e -> raise e)
    ; effc =
        (fun (type a) (e : a Effect.t) ->
          match e with
          | Lwt lwt ->
            Some
              (fun k ->
                let open Lwt.Syntax in
                let* x =
                  Lwt.catch
                    (fun () ->
                      let+ x = lwt () in
                      Ok x)
                    (fun e -> Lwt.return (Error e))
                in
                match x with
                | Ok v -> continue k v
                | Error e -> discontinue k e)
          | _ -> None)
    }

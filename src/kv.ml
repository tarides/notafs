open Lwt_result.Syntax

module Make (Clock : Mirage_clock.PCLOCK) (Check : Checksum.S) (Block : Mirage_block.S) =
struct
  type error =
    [ `Read of Block.error
    | `Write of Block.write_error
    | Mirage_kv.error
    | Mirage_kv.write_error
    | `Invalid_checksum of Int64.t
    | `All_generations_corrupted
    | `Disk_not_formatted
    | `Wrong_page_size of int
    | `Wrong_disk_size of Int64.t
    | `Wrong_checksum_algorithm of string * int
    | `Unsupported_operation of string
    ]

  type write_error = error

  let pp_error h = function
    | `Read e -> Block.pp_error h e
    | `Write e -> Block.pp_write_error h e
    | #Mirage_kv.error as e -> Mirage_kv.pp_error h e
    | #Mirage_kv.write_error as e -> Mirage_kv.pp_write_error h e
    | `Invalid_checksum id -> Format.fprintf h "Invalid_checksum %s" (Int64.to_string id)
    | `All_generations_corrupted -> Format.fprintf h "All_generations_corrupted"
    | `Disk_not_formatted -> Format.fprintf h "Disk_not_formatted"
    | `Wrong_page_size s -> Format.fprintf h "Wrong_page_size %d" s
    | `Wrong_disk_size s -> Format.fprintf h "Wrong_disk_size %s" (Int64.to_string s)
    | `Wrong_checksum_algorithm (name, byte_size) ->
      Format.fprintf h "Wrong_checksum_algorithm (%S, %i)" name byte_size
    | `Unsupported_operation msg -> Format.fprintf h "Unsupported_operation %S" msg

  let pp_write_error = pp_error

  let lift_error lwt : (_, error) Lwt_result.t =
    let open Lwt.Syntax in
    let+ r = lwt in
    match r with
    | Ok v -> Ok v
    | Error `Disk_is_full -> Error `No_space
    | Error (#error as e) -> Error e

  module type S =
    Fs.S
      with type Disk.read_error = Block.error
       and type Disk.write_error = Block.write_error

  type t = T : (module S with type t = 'a) * 'a -> t

  let make_disk block =
    let open Lwt.Syntax in
    let+ (module A_disk) = Context.of_impl (module Block) (module Check) block in
    Ok (module Fs.Make_disk (Clock) (A_disk) : S)

  let format block =
    let open Lwt_result.Syntax in
    let* (module S) = make_disk block in
    let+ (t : S.t) = lift_error @@ S.format () in
    T ((module S), t)

  let connect block =
    let open Lwt_result.Syntax in
    let* (module S) = make_disk block in
    let+ (t : S.t) = lift_error @@ S.connect () in
    T ((module S), t)

  let flush (T ((module S), t)) = lift_error @@ S.flush t
  let clear (T ((module S), _)) = lift_error @@ S.clear ()
  let disk_space (T ((module S), t)) = S.disk_space t
  let free_space (T ((module S), t)) = S.free_space t
  let page_size (T ((module S), t)) = S.page_size t

  type key = Mirage_kv.Key.t

  let exists (T ((module S), t)) key =
    let filename = Mirage_kv.Key.segments key in
    Lwt.return_ok (S.exists t filename)

  let last_modified (T ((module S), t)) key =
    let filename = Mirage_kv.Key.segments key in
    Lwt.return_ok (S.last_modified t filename)

  let get (T ((module S), t)) key =
    let filename = Mirage_kv.Key.segments key in
    let* result = lift_error @@ S.get t filename in
    match result with
    | None -> Lwt_result.fail (`Not_found key)
    | Some contents -> Lwt_result.return contents

  let get_partial (T ((module S), t)) key ~offset ~length =
    let filename = Mirage_kv.Key.segments key in
    let* result = lift_error @@ S.get_partial t filename ~offset ~length in
    match result with
    | None -> Lwt_result.fail (`Not_found key)
    | Some contents -> Lwt_result.return contents

  let list (T ((module S), t)) key =
    let filename = Mirage_kv.Key.segments key in
    let lst = S.list t filename in
    let lst =
      List.map
        (fun (filename, kind) ->
          if kind = `Value && filename = ""
          then key, kind
          else Mirage_kv.Key.( / ) key filename, kind)
        lst
    in
    Lwt.return_ok lst

  let size (T ((module S), t)) key =
    let filename = Mirage_kv.Key.segments key in
    match S.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let+ size = lift_error @@ S.size file in
      Optint.Int63.of_int size

  let allocate (T ((module S), t)) key ?last_modified:_ size =
    let filename = Mirage_kv.Key.segments key in
    match S.find_opt t filename with
    | Some _ -> Lwt_result.fail (`Already_present key)
    | None ->
      let size = Optint.Int63.to_int size in
      let contents = String.make size '\000' in
      let+ _ = lift_error @@ S.touch t filename contents in
      ()

  let set (T ((module S), t)) key contents =
    let filename = Mirage_kv.Key.segments key in
    let* _ = lift_error @@ S.touch t filename contents in
    lift_error @@ S.flush t

  let set_partial (T ((module S), t)) key ~offset contents =
    let filename = Mirage_kv.Key.segments key in
    let* ok = lift_error @@ S.set_partial t filename ~offset contents in
    if ok then lift_error @@ S.flush t else Lwt_result.fail (`Not_found key)

  let remove (T ((module S), t)) key =
    let filename = Mirage_kv.Key.segments key in
    let* () = lift_error @@ S.remove t filename in
    lift_error @@ S.flush t

  let rename (T ((module S), t)) ~source ~dest =
    let src = Mirage_kv.Key.segments source in
    let dst = Mirage_kv.Key.segments dest in
    let* () = lift_error @@ S.rename t ~src ~dst in
    lift_error @@ S.flush t

  let digest _ _ = Lwt_result.fail (`Unsupported_operation "digest")
  let disconnect _ = Lwt.return_unit
end

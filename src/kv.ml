open Lwt_result.Syntax
open Main

module Make (Check : CHECKSUM) (Block : DISK) = struct
  type error =
    [ `Read of Block.error
    | `Write of Block.write_error
    | Mirage_kv.error
    | Mirage_kv.write_error
    | `Invalid_checksum of Int64.t
    | `All_generations_corrupted
    | `Disk_not_formatted
    | `Wrong_page_size of int
    | `Wrong_disk_size
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
    | `Wrong_disk_size -> Format.fprintf h "Wrong_disk_size"
    | `Unsupported_operation msg -> Format.fprintf h "Unsupported_operation %S" msg

  let pp_write_error = pp_error

  let lift_error to_int64 lwt : (_, error) Lwt_result.t =
    let open Lwt.Syntax in
    let+ r = lwt in
    match r with
    | Ok v -> Ok v
    | Error `All_generations_corrupted -> Error `All_generations_corrupted
    | Error `Disk_not_formatted -> Error `Disk_not_formatted
    | Error (`Invalid_checksum id) -> Error (`Invalid_checksum (to_int64 id))
    | Error (`Read e) -> Error (`Read e)
    | Error (`Write e) -> Error (`Write e)
    | Error (`Wrong_page_size s) -> Error (`Wrong_page_size s)
    | Error `Wrong_disk_size -> Error `Wrong_disk_size

  module type S =
    Main.S
      with type Disk.read_error = Block.error
       and type Disk.write_error = Block.write_error

  type t = T : (module S with type t = 'a) * 'a -> t

  let make_disk block =
    let open Lwt.Syntax in
    let+ (module A_disk) = Context.of_impl (module Block) (module Check) block in
    Ok (module Make_disk (A_disk) : S)

  let format block =
    let open Lwt_result.Syntax in
    let* (module S) = make_disk block in
    let+ (t : S.t) = lift_error S.Disk.Id.to_int64 @@ S.format () in
    T ((module S), t)

  let connect block =
    let open Lwt_result.Syntax in
    let* (module S) = make_disk block in
    let+ (t : S.t) = lift_error S.Disk.Id.to_int64 @@ S.of_block () in
    T ((module S), t)

  let flush (T ((module S), t)) = lift_error S.Disk.Id.to_int64 @@ S.flush t
  let disk_space (T ((module S), t)) = S.disk_space t
  let free_space (T ((module S), t)) = S.free_space t
  let page_size (T ((module S), t)) = S.page_size t

  type key = Mirage_kv.Key.t

  let exists (T ((module S), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    match S.find_opt t filename with
    | None -> Lwt.return_ok None
    | Some _ -> Lwt.return_ok (Some `Value)

  let get (T ((module S), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    match S.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let* size = lift_error S.Disk.Id.to_int64 @@ S.size file in
      let bytes = Bytes.create size in
      let+ quantity =
        lift_error S.Disk.Id.to_int64 @@ S.blit_to_bytes file bytes ~off:0 ~len:size
      in
      assert (quantity = size) ;
      Bytes.unsafe_to_string bytes

  let get_partial (T ((module S), t)) key ~offset ~length =
    let filename = Mirage_kv.Key.to_string key in
    match S.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let* size = lift_error S.Disk.Id.to_int64 @@ S.size file in
      let off = Optint.Int63.to_int offset in
      assert (off >= 0) ;
      assert (off + length <= size) ;
      let bytes = Bytes.create length in
      let+ quantity =
        lift_error S.Disk.Id.to_int64 @@ S.blit_to_bytes file bytes ~off ~len:length
      in
      assert (quantity = size) ;
      Bytes.unsafe_to_string bytes

  let list (T ((module S), t)) key =
    let filename = Mirage_kv.Key.to_string key in
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
    let filename = Mirage_kv.Key.to_string key in
    match S.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let+ size = lift_error S.Disk.Id.to_int64 @@ S.size file in
      Optint.Int63.of_int size

  let allocate (T ((module S), t)) key ?last_modified:_ size =
    let filename = Mirage_kv.Key.to_string key in
    match S.find_opt t filename with
    | Some _ -> Lwt_result.fail (`Already_present key)
    | None ->
      let size = Optint.Int63.to_int size in
      let contents = String.make size '\000' in
      let+ _ = lift_error S.Disk.Id.to_int64 @@ S.touch t filename contents in
      ()

  let set (T ((module S), t)) key contents =
    let filename = Mirage_kv.Key.to_string key in
    let* () =
      match S.find_opt t filename with
      | None -> Lwt_result.return ()
      | Some _ -> lift_error S.Disk.Id.to_int64 @@ S.remove t filename
    in
    let* _ = lift_error S.Disk.Id.to_int64 @@ S.touch t filename contents in
    lift_error S.Disk.Id.to_int64 @@ S.flush t

  let set_partial (T ((module S), t)) key ~offset contents =
    let filename = Mirage_kv.Key.to_string key in
    match S.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let off = Optint.Int63.to_int offset in
      let len = String.length contents in
      let* _ =
        lift_error S.Disk.Id.to_int64 @@ S.blit_from_string t file ~off ~len contents
      in
      lift_error S.Disk.Id.to_int64 @@ S.flush t

  let remove (T ((module S), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    let* () = lift_error S.Disk.Id.to_int64 @@ S.remove t filename in
    lift_error S.Disk.Id.to_int64 @@ S.flush t

  let rename (T ((module S), t)) ~source ~dest =
    let src = Mirage_kv.Key.to_string source in
    let dst = Mirage_kv.Key.to_string dest in
    let* () = lift_error S.Disk.Id.to_int64 @@ S.rename t ~src ~dst in
    lift_error S.Disk.Id.to_int64 @@ S.flush t

  let last_modified _ _ = Lwt_result.fail (`Unsupported_operation "last_modified")
  let digest _ _ = Lwt_result.fail (`Unsupported_operation "digest")
  let disconnect _ = Lwt.return_unit
  let stats (T ((module S), _)) = Stats.snapshot S.Disk.stats
end

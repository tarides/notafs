open Mirage

(* This needs to be included until support for notafs is merged
   in the upstream mirage tool. *)
open (
  struct
    type checksum = CHECKSUM

    let checksum_t = Type.v CHECKSUM

    let notafs_kv_rw_conf ~format =
      (* TODO remove pin when notafs is published on opam *)
      let packages =
        [ package ~pin:"git+https://github.com/tarides/notafs.git" "notafs" ]
      in
      let connect _ modname = function
        | [ _pclock_v; _checksum; block_v ] ->
            let connect_c = Fmt.str "%s.connect %s" modname block_v in
            let format_c = Fmt.str "%s.format %s" modname block_v in
            let connect_format_c =
              match format with
              | `Never -> connect_c
              | `Always -> format_c
              | `If_not ->
                  Fmt.str
                    {ml|%s >>= function
                        | Error `Disk_not_formatted -> %s
                        | x -> Lwt.return x|ml}
                    connect_c format_c
            in
            code ~pos:__POS__
              {ml|(%s)
                >|= Result.map_error (Fmt.str "notafs_kv_rw: %%a" %s.pp_error)
                >|= Result.fold ~ok:Fun.id ~error:failwith|ml}
              connect_format_c modname
        | _ -> connect_err "notafs_kv_rw" 3
      in
      impl ~packages ~connect "Notafs.KV"
        (pclock @-> checksum_t @-> block @-> kv_rw)

    let notafs_kv_rw ?(pclock = default_posix_clock) ?(checksum = `Adler32)
        ?(format = `If_not) block =
      let checksum_modname =
        match checksum with
        | `Adler32 -> "Notafs.Adler32"
        | `No_checksum -> "Notafs.No_checksum"
      in
      let checksum = impl checksum_modname checksum_t in
      notafs_kv_rw_conf ~format $ pclock $ checksum $ block
  end :
    sig
      val notafs_kv_rw :
        ?pclock:pclock impl ->
        ?checksum:[ `Adler32 | `No_checksum ] ->
        ?format:[ `Always | `Never | `If_not ] ->
        block impl ->
        kv_rw impl
      (** [notafs_kv_rw ~checksum ~format block] exposes a KV_RW interface from
          a notafs block, with the given checksum mechanism. The underlying
          block is expected to be a well-formed notafs volume if
          [format = `Never], is always formatted (and cleared) to be one if
          [format = `Always], or only as needed (the first time it's opened) if
          [format = `If_not] (the default). *)
    end)

let main = main "Unikernel.Main" (kv_rw @-> job)

let block =
  if_impl Key.is_solo5 (block_of_file "storage") (block_of_file "/tmp/storage")

let kv = notafs_kv_rw block
let () = register "block_test" [ main $ kv ]

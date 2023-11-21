open Lwt.Syntax

module Main (Clock : Mirage_clock.MCLOCK) (Block : Mirage_block.S) = struct
  module Conf = struct
    let entries = 32
    let stable_hash = 256
    let contents_length_header = Some `Varint
    let inode_child_order = `Seeded_hash
    let forbid_empty_dir_persistence = true
  end

  module Schema = struct
    open Irmin
    module Metadata = Metadata.None
    module Contents = Contents.String_v2
    module Path = Path.String_list
    module Branch = Branch.String
    module Hash = Hash.SHA1
    module Node = Node.Generic_key.Make_v2 (Hash) (Path) (Metadata)
    module Commit = Commit.Generic_key.Make_v2 (Hash)
    module Info = Info.Default
  end

  module Store = struct
    module Maker = Irmin_pack_notafs.Maker (Clock) (Block) (Conf)
    include Maker.Make (Schema)

    let config ?(readonly = false) ?(fresh = true) root =
      Irmin_pack.config
        ~readonly
        ~fresh
        ~indexing_strategy:Irmin_pack.Indexing_strategy.minimal
        root
  end

  module Fs = Store.Maker.Fs
  module Io = Store.Maker.Io

  let info () = Store.Info.v Int64.zero ~message:"test"
  let fresh = false

  let start _ b =
    let* () =
      if fresh
      then
        let+ _ = Fs.format b in
        ()
      else Lwt.return_unit
    in
    let* () = Io.init b in
    Lwt_direct.indirect
    @@ fun () ->
    Eio_mock.Backend.run
    @@ fun () ->
    let repo = Store.Repo.v (Store.config ~fresh "/") in
    let main = Store.main repo in
    let counter =
      match Store.get main [ "counter" ] with
      | contents -> int_of_string contents
      | exception Invalid_argument _ -> 0
    in
    Format.printf "counter=%i@." counter ;
    Store.set_exn ~info main [ "counter" ] (string_of_int (counter + 1)) ;
    Format.printf "Latest commit is %a@." Store.Commit.pp_hash (Store.Head.get main) ;
    Store.Repo.close repo
end

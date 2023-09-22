module B = Block_viz.Make (Block)

module Clock = struct
  let elapsed_ns () = Mtime.Span.to_uint64_ns (Mtime_clock.elapsed ())
  let period_ns () = None
end

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
  module Maker = Irmin_pack_notafs.Maker (Clock) (B) (Conf)
  include Maker.Make (Schema)

  let config ?(readonly = false) ?(fresh = true) root =
    Irmin_pack.config
      ~readonly
      ~fresh
      ~indexing_strategy:Irmin_pack.Indexing_strategy.minimal
      root
end

let root = "/tmp/notafs-irmin"

module Fs = Store.Maker.Fs
module Io = Store.Maker.Io

let main ~fresh () =
  let is_connected = ref false in
  let connect path =
    Lwt_direct.direct
    @@ fun () ->
    let open Lwt.Syntax in
    assert (not !is_connected) ;
    let* r = Block.connect ~prefered_sector_size:(Some 1024) path in
    let+ r = B.of_block r in
    is_connected := true ;
    r
  in
  let disconnect block =
    assert !is_connected ;
    let _ = Lwt_direct.direct @@ fun () -> B.disconnect block in
    is_connected := false
  in
  let () =
    if fresh
    then begin
      let block = connect root in
      let _ = Lwt_direct.direct @@ fun () -> Fs.format block in
      disconnect block
    end
  in
  let block = connect root in
  let () = Lwt_direct.direct @@ fun () -> Io.init block in
  let repo = Store.Repo.v (Store.config ~fresh "/") in
  Store.Repo.close repo ;
  Io.notafs_flush () ;
  disconnect block ;
  let block = connect root in
  let () = Lwt_direct.direct @@ fun () -> Io.init block in
  let with_store fn =
    let repo = Store.Repo.v (Store.config ~fresh:false "/") in
    let main = Store.main repo in
    Fun.protect (fun () -> fn repo main) ~finally:(fun () -> Store.Repo.close repo)
  in
  (*
     let () = Lwt_direct.direct @@ fun () -> Io.init block in
     let repo = Store.Repo.v (Store.config ~fresh:false "/") in
     let with_store fn =
       let main = Store.main repo in
       fn repo main
     in
  *)
  begin
    with_store
    @@ fun _repo main ->
    Store.set_exn
      main
      ~info:(fun () -> Store.Info.v Int64.zero ~message:"test")
      [ "hello" ]
      "world"
  end ;
  let gc_commits = ref [] in
  let gc_run = ref 0 in
  let do_gc () =
    incr gc_run;
    B.draw_status block (Printf.sprintf "garbage collect START") ;
    match List.rev !gc_commits with
    | commit :: rest ->
      gc_commits := List.rev rest ;
      with_store
      @@ fun repo _main ->
      (match Store.Gc.start_exn ~unlink:true repo commit with
       | status ->
         Format.printf "GC run %d: %b@." !gc_run status ;
         B.draw_status block (Printf.sprintf "garbage collect FINALISE") ;
         let _ = Store.Gc.finalise_exn ~wait:false repo in
         B.draw_status block (Printf.sprintf "garbage collect DONE") ;
         ())
    | [] -> failwith "no gc commits"
  in
  let prev = ref None in
  for i = 0 to 1_000 do
    begin
      with_store
      @@ fun repo main ->
      let hash_str = Repr.to_string (Store.Commit.t repo) (Store.Head.get main) in
      begin
        match !prev with
        | None -> ()
        | Some h -> assert (h = hash_str)
      end ;
      Store.set_exn
        main
        ~info:(fun () -> Store.Info.v Int64.zero ~message:"more test")
        [ "a" ]
        (string_of_int i ^ String.make 500 (Char.chr (128 + (i mod 128)))) ;
      let new_hash_str = Repr.to_string (Store.Commit.t repo) (Store.Head.get main) in
      prev := Some new_hash_str ;
      Format.printf "New commit is %a@." Store.Commit.pp_hash (Store.Head.get main) ;
      B.draw_status block (Printf.sprintf "commit %i" i) ;
      let _str = Store.get main [ "a" ] in
      if i > 0 && i mod 20 = 0
      then begin
        B.draw_status block (Printf.sprintf "GC chunk split") ;
        Store.split repo ;
        let current_commit = Store.Commit.key @@ Store.Head.get main in
        gc_commits := current_commit :: !gc_commits
      end
    end ;
    if i > 100 && i mod 20 = 0 then do_gc ()
  done ;
  ()

(* cmdliner *)
open Cmdliner

let sleep =
  Arg.(
    value & opt float (0.) & info [ "s"; "sleep" ] ~docv:"sleep" ~doc:"sleep time in seconds")

let main sleep =
  B.sleep := sleep;
  Lwt_main.run
  @@ Lwt_direct.indirect
  @@ fun () -> Eio_mock.Backend.run @@ main ~fresh:true

let main_cmd =
  let info = Cmd.info "graphics" in
  Cmd.v info Term.(const main $ sleep)

let () = exit (Cmd.eval ~catch:false main_cmd)

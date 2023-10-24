open Lwt_result.Syntax
module Stats = Stats

module type DISK = Context.DISK
module type CHECKSUM = Context.CHECKSUM

module type S = sig
  module Disk : Context.A_DISK

  type t
  type error
  type 'a io := ('a, error) Lwt_result.t

  val format : unit -> t io
  val of_block : unit -> t io
  val flush : t -> unit io

  type file

  val filename : file -> string
  val size : file -> int io
  val mem : t -> string -> bool
  val append_substring : file -> string -> off:int -> len:int -> unit io
  val blit_from_string : t -> file -> off:int -> len:int -> string -> unit io
  val blit_to_bytes : file -> off:int -> len:int -> bytes -> int io
  val rename : t -> src:string -> dst:string -> unit io
  val touch : t -> string -> string -> file io
  val replace : t -> string -> string -> unit io
  val remove : t -> string -> unit io
  val find_opt : t -> string -> file option
  val list : t -> string -> (string * [ `Value | `Dictionary ]) list
end

module Make_disk (B : Context.A_DISK) : S with type error = B.error = struct
  module Disk = B
  module Sector = Sector.Make (B)
  module Root = Root.Make (B)
  module Queue = Queue.Make (B)
  module Files = Files.Make (B)
  module Rope = Rope.Make (B)

  type error = B.error

  type t =
    { root : Root.t
    ; mutable files : Files.t
    ; mutable free_queue : Queue.q
    }

  let unsafe_of_root root =
    let* payload = Root.get_payload root in
    let* free_queue = Root.get_free_queue root in
    let* files = Files.load payload in
    let* free_queue = Queue.load free_queue in
    let* () = Files.verify_checksum files in
    let+ () = Queue.verify_checksum free_queue in
    { root; files; free_queue }

  let rec of_root root rollback_nb =
    let open Lwt.Syntax in
    let* t = unsafe_of_root root in
    match t with
    | Error (`Invalid_checksum _) ->
      if rollback_nb > Root.nb
      then Lwt_result.fail `All_generations_corrupted
      else begin
        Root.pred_gen root ;
        of_root root (rollback_nb + 1)
      end
    | r -> Lwt_result.lift r

  let of_root root =
    let+ t = of_root root 0 in
    (B.allocator
       := fun required ->
            let t_free_queue = t.free_queue in
            let+ free_queue, allocated = Queue.pop_front t_free_queue required in
            assert (t.free_queue == t_free_queue) ;
            t.free_queue <- free_queue ;
            allocated) ;
    t

  let format () =
    let* root = Root.format () in
    of_root root

  let of_block () =
    let* root = Root.load () in
    of_root root

  let flush t =
    assert !B.safe_lru ;
    B.safe_lru := false ;
    let* required = Files.count_new t.files in
    let+ () =
      if required = 0
      then begin
        assert (B.acquire_discarded () = []) ;
        Lwt_result.return ()
      end
      else begin
        let t_free_queue = t.free_queue in
        let* free_queue = Queue.push_discarded t.free_queue in
        let* free_queue, allocated = Queue.pop_front free_queue required in
        assert (List.length allocated = required) ;
        let* free_queue, to_flush_queue = Queue.self_allocate ~free_queue in
        let* payload_root, to_flush, allocated = Files.to_payload t.files allocated in
        assert (allocated = []) ;
        let* () = Root.flush (List.rev_append to_flush_queue to_flush) in
        let+ () = Root.update t.root ~queue:free_queue ~payload:payload_root in
        assert (B.acquire_discarded () = []) ;
        assert (t.free_queue == t_free_queue) ;
        t.free_queue <- free_queue ;
        B.flush ()
      end
    in
    B.safe_lru := true ;
    ()

  let filename t = fst t

  let append_substring (_filename, rope) str ~off ~len =
    let+ t_rope = Rope.append_from !rope (str, off, off + len) in
    rope := t_rope

  let rename t ~src ~dst =
    let+ files = Files.rename t.files ~src ~dst in
    t.files <- files

  let size file = Files.size file
  let mem t file = Files.mem t.files file

  let remove t filename =
    let+ files = Files.remove t.files filename in
    t.files <- files

  let touch t filename str =
    assert (not (mem t filename)) ;
    let+ rope = Rope.of_string str in
    let r = ref rope in
    t.files <- Files.add t.files filename r ;
    filename, r

  let replace t filename str =
    assert (mem t filename) ;
    let prev = Files.find t.files filename in
    let* () = Rope.free !prev in
    let+ rope = Rope.of_string str in
    prev := rope

  let blit_to_bytes filename ~off ~len bytes =
    Files.blit_to_bytes filename ~off ~len bytes

  let blit_from_string t filename ~off ~len bytes =
    Files.blit_from_string t.files filename ~off ~len bytes

  let find_opt t filename =
    match Files.find_opt t.files filename with
    | None -> None
    | Some file -> Some (filename, file)

  type file = Files.file

  let list t prefix = Files.list t.files prefix
end

module Make_check (Check : CHECKSUM) (B : DISK) = struct
  let debug = false

  type t = T : (module S with type t = 'a) * 'a -> t

  open Lwt.Syntax

  let catch fn =
    Lwt.catch fn (fun e ->
      Format.printf "ERROR: %s@." (Printexc.to_string e) ;
      Printexc.print_backtrace stdout ;
      Format.printf "@." ;
      raise e)

  let catch' fn =
    try fn () with
    | e ->
      Format.printf "ERROR: %s@." (Printexc.to_string e) ;
      Printexc.print_backtrace stdout ;
      Format.printf "@." ;
      raise e

  let or_fail s lwt =
    Lwt.map
      (function
       | Ok r -> r
       | Error _ -> failwith s)
      lwt

  let format block =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.format@." ;
    let* (module A_disk) = Context.of_impl (module B) (module Check) block in
    let (module S) = (module Make_disk (A_disk) : S) in
    or_fail "Notafs.format"
    @@
    let open Lwt_result.Syntax in
    let+ (t : S.t) = S.format () in
    T ((module S), t)

  let stats (T ((module S), _)) = Stats.snapshot S.Disk.stats

  let of_block block =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.of_block@." ;
    let* (module A_disk) = Context.of_impl (module B) (module Check) block in
    let (module S) = (module Make_disk (A_disk) : S) in
    or_fail "Notafs.of_block"
    @@
    let open Lwt_result.Syntax in
    let+ (t : S.t) = S.of_block () in
    T ((module S), t)

  let flush (T ((module X), t)) =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.flush@." ;
    or_fail "Notafs.flush" @@ X.flush t

  let mem (T ((module X), t)) filename =
    catch'
    @@ fun () ->
    if debug then Format.printf "Notafs.mem@." ;
    X.mem t filename

  type file = File : (module S with type t = 'a and type file = 'b) * 'a * 'b -> file

  let find (T ((module X), t)) filename =
    catch'
    @@ fun () ->
    if debug then Format.printf "Notafs.find %S@." filename ;
    match X.find_opt t filename with
    | None -> None
    | Some file -> Some (File ((module X), t, file))

  let filename (File ((module X), _, file)) =
    catch'
    @@ fun () ->
    if debug then Format.printf "Notafs.filename@." ;
    X.filename file

  let remove (T ((module X), t)) filename =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.remove@." ;
    or_fail "Notafs.remove" @@ X.remove t filename

  let replace (T ((module X), t)) filename contents =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.replace@." ;
    or_fail "Notafs.replace" @@ X.replace t filename contents

  let touch (T ((module X), t)) filename contents =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.touch %S@." filename ;
    or_fail "Notafs.touch"
    @@
    let open Lwt_result.Syntax in
    let+ file = X.touch t filename contents in
    File ((module X), t, file)

  let rename (T ((module X), t)) ~src ~dst =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.rename@." ;
    or_fail "Notafs.rename" @@ X.rename t ~src ~dst

  let append_substring (File ((module X), _, file)) str ~off ~len =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.append_substring@." ;
    or_fail "Notafs.append_substring" @@ X.append_substring file str ~off ~len

  let blit_to_bytes (File ((module X), _, file)) ~off ~len bytes =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.blit_to_bytes@." ;
    or_fail "Notafs.blit_to_bytes" @@ X.blit_to_bytes file ~off ~len bytes

  let blit_from_string (File ((module X), t, file)) ~off ~len string =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.blit_from_string@." ;
    or_fail "Notafs.blit_from_string" @@ X.blit_from_string t file ~off ~len string

  let size (File ((module X), _, file)) =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.size@." ;
    or_fail "Notafs.size"
    @@
    let open Lwt_result.Syntax in
    let+ r = X.size file in
    if debug then Format.printf "Notafs.size: %i@." r ;
    r
end

module No_checksum = struct
  type t = unit

  let equal = ( = )
  let default = ()
  let digest_bigstring _ _ _ _ = ()
  let to_int32 _ = Int32.zero
  let of_int32 _ = ()
  let byte_size = 0
  let read _ _ = ()
  let write _ _ _ = ()
end

module Adler32 = struct
  include Checkseum.Adler32

  let byte_size = 8
  let read cstruct offset = of_int32 @@ Cstruct.HE.get_uint32 cstruct offset
  let write cstruct offset v = Cstruct.HE.set_uint32 cstruct offset (to_int32 v)
end

module Make (B : DISK) = Make_check (Adler32) (B)

module Make_kv (Check : CHECKSUM) (Block : DISK) : sig
  include Mirage_kv.RW

  val format : Block.t -> (t, write_error) Lwt_result.t
  val flush : t -> (unit, write_error) Lwt_result.t
  val stats : t -> Stats.ro
end = struct
  type block_error =
    [ `Read of Block.error
    | `Write of Block.write_error
    ]

  type error =
    [ Mirage_kv.error
    | Mirage_kv.write_error
    | block_error
    | `Unsupported_operation of string
    | `Disk_failed
    ]

  let pp_error h = function
    | #Mirage_kv.error as e -> Mirage_kv.pp_error h e
    | #Mirage_kv.write_error as e -> Mirage_kv.pp_write_error h e
    | `Unsupported_operation msg -> Format.fprintf h "Unsupported_operation %S" msg
    | `Disk_failed -> Format.fprintf h "Disk_failed"
    | `Read e -> Block.pp_error h e
    | `Write e -> Block.pp_write_error h e

  type write_error = error

  let pp_write_error = pp_error

  let lift_error lwt =
    let open Lwt.Syntax in
    let+ r = lwt in
    match r with
    | Error _ -> Error `Disk_failed
    | Ok v -> Ok v

  type t = T : (module S with type t = 'a) * 'a -> t

  let format block =
    let open Lwt.Syntax in
    let* (module A_disk) = Context.of_impl (module Block) (module Check) block in
    let (module S) = (module Make_disk (A_disk) : S) in
    let open Lwt_result.Syntax in
    let+ (t : S.t) = lift_error @@ S.format () in
    T ((module S), t)

  let flush (T ((module X), t)) = lift_error @@ X.flush t

  type key = Mirage_kv.Key.t

  let exists (T ((module X), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    match X.find_opt t filename with
    | None -> Lwt.return_ok None
    | Some _ -> Lwt.return_ok (Some `Value)

  let get (T ((module X), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    match X.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let* size = lift_error @@ X.size file in
      let bytes = Bytes.create size in
      let+ quantity = lift_error @@ X.blit_to_bytes file bytes ~off:0 ~len:size in
      assert (quantity = size) ;
      Bytes.unsafe_to_string bytes

  let get_partial (T ((module X), t)) key ~offset ~length =
    let filename = Mirage_kv.Key.to_string key in
    match X.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let* size = lift_error @@ X.size file in
      let off = Optint.Int63.to_int offset in
      assert (off >= 0) ;
      assert (off + length <= size) ;
      let bytes = Bytes.create length in
      let+ quantity = lift_error @@ X.blit_to_bytes file bytes ~off ~len:length in
      assert (quantity = size) ;
      Bytes.unsafe_to_string bytes

  let list (T ((module X), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    let lst =
      List.map
        (fun (filename, kind) -> Mirage_kv.Key.( / ) key filename, kind)
        (X.list t filename)
    in
    Lwt.return_ok lst

  let size (T ((module X), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    match X.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let+ size = lift_error @@ X.size file in
      Optint.Int63.of_int size

  let allocate (T ((module X), t)) key ?last_modified:_ size =
    let filename = Mirage_kv.Key.to_string key in
    match X.find_opt t filename with
    | Some _ -> Lwt_result.fail (`Already_present key)
    | None ->
      let size = Optint.Int63.to_int size in
      let contents = String.make size '\000' in
      let+ _ = lift_error @@ X.touch t filename contents in
      ()

  let set (T ((module X), t)) key contents =
    let filename = Mirage_kv.Key.to_string key in
    let* () =
      match X.find_opt t filename with
      | None -> Lwt_result.return ()
      | Some _ -> lift_error @@ X.remove t filename
    in
    let* _ = lift_error @@ X.touch t filename contents in
    lift_error @@ X.flush t

  let set_partial (T ((module X), t)) key ~offset contents =
    let filename = Mirage_kv.Key.to_string key in
    match X.find_opt t filename with
    | None -> Lwt_result.fail (`Not_found key)
    | Some file ->
      let off = Optint.Int63.to_int offset in
      let len = String.length contents in
      let* _ = lift_error @@ X.blit_from_string t file ~off ~len contents in
      lift_error @@ X.flush t

  let remove (T ((module X), t)) key =
    let filename = Mirage_kv.Key.to_string key in
    let* () = lift_error @@ X.remove t filename in
    lift_error @@ X.flush t

  let rename (T ((module X), t)) ~source ~dest =
    let src = Mirage_kv.Key.to_string source in
    let dst = Mirage_kv.Key.to_string dest in
    let* () = lift_error @@ X.rename t ~src ~dst in
    lift_error @@ X.flush t

  let last_modified _ _ = Lwt_result.fail (`Unsupported_operation "last_modified")
  let digest _ _ = Lwt_result.fail (`Unsupported_operation "digest")
  let disconnect _ = Lwt.return_unit
  let stats (T ((module S), _)) = Stats.snapshot S.Disk.stats
end

module KV (Block : DISK) = Make_kv (No_checksum) (Block)

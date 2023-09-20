open Lwt_result.Syntax
module Sector = Sector

module type DISK = Context.DISK

module type S = sig
  type t
  type error

  val format : unit -> (t, error) Lwt_result.t
  val of_block : unit -> (t, error) Lwt_result.t
  val flush : t -> (unit, error) Lwt_result.t

  type file

  val filename : file -> string
  val size : file -> int
  val mem : t -> string -> bool

  val append_substring
    :  t
    -> file
    -> string
    -> off:int
    -> len:int
    -> (unit, error) Lwt_result.t

  val blit_from_string
    :  t
    -> file
    -> off:int
    -> len:int
    -> string
    -> (unit, error) Lwt_result.t

  val blit_to_bytes : file -> off:int -> len:int -> bytes -> (int, error) Lwt_result.t
  val rename : t -> src:string -> dst:string -> (unit, error) Lwt_result.t
  val touch : t -> string -> string -> (file, error) Lwt_result.t
  val replace : t -> string -> string -> (unit, error) Lwt_result.t
  val remove : t -> string -> (unit, error) Lwt_result.t
  val find_opt : t -> string -> file option
end

module Make_disk (B : Context.A_DISK) : S with type error = B.error = struct
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

  let of_root root =
    let* files = Files.load (Root.get_payload root) in
    let+ free_queue = Queue.load (Root.get_free_queue root) in
    { root; files; free_queue }

  let format () =
    let* root = Root.format () in
    of_root root

  let of_block () =
    let* root = Root.load () in
    of_root root

  let flush t =
    let* required = Files.count_new t.files in
    if required = 0
    then Lwt_result.return ()
    else begin
      let* free_queue = Queue.push_discarded t.free_queue in
      let* free_queue, allocated = Queue.pop_front free_queue required in
      assert (List.length allocated = required) ;
      let* free_queue, to_flush_queue = Queue.self_allocate ~free_queue in
      let* payload_root, to_flush, allocated = Files.to_payload t.files allocated in
      assert (allocated = []) ;
      let* () = Root.flush (List.rev_append to_flush_queue to_flush) in
      let+ () = Root.update t.root ~queue:free_queue ~payload:payload_root in
      assert (B.acquire_discarded () = []) ;
      t.free_queue <- free_queue ;
      B.flush ()
    end

  let filename t = fst t

  let append _t (_filename, rope) str =
    let+ t_rope = Rope.append !rope str in
    rope := t_rope

  let append_substring t filename str ~off ~len =
    let str = String.sub str off len in
    append t filename str

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
end

module Make (B : DISK) = struct
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

  let or_fail lwt =
    Lwt.map
      (function
       | Ok r -> r
       | Error _ -> failwith "fail")
      lwt

  let format block =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.format@." ;
    let* (module A_disk) = Context.of_impl (module B) block in
    let (module S) = (module Make_disk (A_disk) : S) in
    or_fail
    @@
    let open Lwt_result.Syntax in
    let+ (t : S.t) = S.format () in
    T ((module S), t)

  let of_block block =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.of_block@." ;
    let* (module A_disk) = Context.of_impl (module B) block in
    let (module S) = (module Make_disk (A_disk) : S) in
    or_fail
    @@
    let open Lwt_result.Syntax in
    let+ (t : S.t) = S.of_block () in
    T ((module S), t)

  let flush (T ((module X), t)) =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.flush@." ;
    or_fail @@ X.flush t

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
    or_fail @@ X.remove t filename

  let replace (T ((module X), t)) filename contents =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.replace@." ;
    or_fail @@ X.replace t filename contents

  let touch (T ((module X), t)) filename contents =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.touch %S@." filename ;
    or_fail
    @@
    let open Lwt_result.Syntax in
    let+ file = X.touch t filename contents in
    File ((module X), t, file)

  let rename (T ((module X), t)) ~src ~dst =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.rename@." ;
    or_fail @@ X.rename t ~src ~dst

  let append_substring (File ((module X), t, file)) str ~off ~len =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.append_substring@." ;
    or_fail @@ X.append_substring t file str ~off ~len

  let blit_to_bytes (File ((module X), _, file)) ~off ~len bytes =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.blit_to_bytes@." ;
    or_fail @@ X.blit_to_bytes file ~off ~len bytes

  let blit_from_string (File ((module X), t, file)) ~off ~len string =
    catch
    @@ fun () ->
    if debug then Format.printf "Notafs.blit_from_string@." ;
    or_fail @@ X.blit_from_string t file ~off ~len string

  let size (File ((module X), _, file)) =
    catch'
    @@ fun () ->
    if debug then Format.printf "Notafs.size@." ;
    let r = X.size file in
    if debug then Format.printf "Notafs.size: %i@." r ;
    r
end

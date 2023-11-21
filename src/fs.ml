open Lwt_result.Syntax

module type CHECKSUM = Checksum.S

module type S = sig
  module Disk : Context.A_DISK

  type t
  type error = Disk.error
  type 'a io := ('a, error) Lwt_result.t

  val pp_error : Format.formatter -> error -> unit
  val format : unit -> t io
  val connect : unit -> t io
  val flush : t -> unit io
  val clear : unit -> unit io
  val disk_space : t -> int64
  val free_space : t -> int64
  val page_size : t -> int

  type key = string list
  type file

  val filename : file -> string
  val size : file -> int io
  val exists : t -> key -> [> `Dictionary | `Value ] option
  val append_substring : t -> file -> string -> off:int -> len:int -> unit io
  val blit_from_string : t -> file -> off:int -> len:int -> string -> unit io
  val blit_to_bytes : t -> file -> off:int -> len:int -> bytes -> int io
  val rename : t -> src:key -> dst:key -> unit io
  val touch : t -> key -> string -> file io
  val remove : t -> key -> unit io
  val find_opt : t -> key -> file option
  val list : t -> key -> (string * [ `Value | `Dictionary ]) list
end

module Make_disk (B : Context.A_DISK) : S with module Disk = B = struct
  module Disk = B
  module Sector = Sector.Make (B)
  module Root = Root.Make (B)
  module Queue = Queue.Make (B)
  module Files = Files.Make (B)
  module Rope = Rope.Make (B)

  type error = B.error

  let pp_error = B.pp_error

  type key = string list

  type t =
    { root : Root.t
    ; mutable files : Files.t
    ; mutable free_queue : Queue.q
    ; lock : Lwt_mutex.t
    }

  let free_space t = t.free_queue.free_sectors
  let disk_space _ = B.nb_sectors
  let page_size _ = B.page_size

  let unsafe_of_root root =
    let* payload = Root.get_payload root in
    let* free_queue = Root.get_free_queue root in
    let* files = Files.load payload in
    let* free_queue = Queue.load free_queue in
    let* () = Files.verify_checksum files in
    let* () = Queue.verify_checksum free_queue in
    let+ () = B.clear () in
    let lock = Lwt_mutex.create () in
    { root; files; free_queue; lock }

  let reachable_size t =
    let roots = Root.reachable_size t.root in
    let* payload =
      let* root_payload = Root.get_payload t.root in
      if Sector.is_null_ptr root_payload
      then Lwt_result.return 0
      else Files.reachable_size t.files
    in
    let+ queue =
      let* _, root_queue, _ = Root.get_free_queue t.root in
      if Sector.is_null_ptr root_queue
      then Lwt_result.return 0
      else Queue.reachable_size t.free_queue
    in
    roots + queue + payload

  let check_size t =
    (*
       let* rs = reachable_size t in
       let+ s = Queue.size t.free_queue in
       let expected_free =
       Int64.add
       (Int64.sub B.nb_sectors (B.Id.to_int64 t.free_queue.free_start))
       (Int64.of_int s)
       in
       let used_space = Int64.to_int (Int64.sub B.nb_sectors expected_free) in
       assert (used_space = rs) ;
       assert (Int64.equal t.free_queue.free_sectors expected_free)
    *)
    ignore reachable_size ;
    ignore t ;
    Lwt_result.return ()

  let of_root root =
    let* t = unsafe_of_root root in
    let+ () = check_size t in
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

  let connect () = Root.load ~check:of_root ()
  let with_lock t fn = Lwt_mutex.with_lock t.lock fn
  let clear () = B.clear ()

  let flush t =
    with_lock t
    @@ fun () ->
    B.protect_lru
    @@ fun () ->
    let* required = Files.count_new t.files in
    let* () =
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
        let* free_queue = Root.update t.root ~queue:free_queue ~payload:payload_root in
        assert (B.acquire_discarded () = []) ;
        assert (t.free_queue == t_free_queue) ;
        t.free_queue <- free_queue ;
        check_size t
      end
    in
    B.clear ()

  let filename t = Files.filename (fst t)

  let append_substring t (_filename, rope) str ~off ~len =
    with_lock t
    @@ fun () ->
    let+ t_rope = Rope.append_from !rope (str, off, off + len) in
    rope := t_rope

  let rename t ~src ~dst =
    with_lock t
    @@ fun () ->
    let+ files = Files.rename t.files ~src ~dst in
    t.files <- files

  let size file = Files.size file
  let exists t file = Files.exists t.files file

  let remove t filename =
    with_lock t
    @@ fun () ->
    let+ files = Files.remove t.files filename in
    t.files <- files

  let touch t filename str =
    with_lock t
    @@ fun () ->
    assert (Option.is_none @@ exists t filename) ;
    let* rope = Rope.of_string str in
    let r = ref rope in
    let+ files = Files.add t.files filename r in
    t.files <- files ;
    filename, r

  let blit_to_bytes t filename ~off ~len bytes =
    with_lock t @@ fun () -> Files.blit_to_bytes filename ~off ~len bytes

  let blit_from_string t filename ~off ~len bytes =
    with_lock t @@ fun () -> Files.blit_from_string t.files filename ~off ~len bytes

  let find_opt t filename =
    match Files.find_opt t.files filename with
    | None -> None
    | Some file -> Some (filename, file)

  type file = key * Files.file

  let list t prefix = Files.list t.files prefix
end

module Make_check (Check : CHECKSUM) (Block : Mirage_block.S) = struct
  let debug = false

  type error =
    [ `Read of Block.error
    | `Write of Block.write_error
    | `Invalid_checksum of Int64.t
    | `All_generations_corrupted
    | `Disk_is_full
    | `Disk_not_formatted
    | `Wrong_page_size of int
    | `Wrong_disk_size of Int64.t
    | `Wrong_checksum_algorithm of string * int
    ]

  let pp_error h = function
    | `Read e -> Block.pp_error h e
    | `Write e -> Block.pp_write_error h e
    | `Invalid_checksum id -> Format.fprintf h "Invalid_checksum %s" (Int64.to_string id)
    | `All_generations_corrupted -> Format.fprintf h "All_generations_corrupted"
    | `Disk_is_full -> Format.fprintf h "Disk_is_full"
    | `Disk_not_formatted -> Format.fprintf h "Disk_not_formatted"
    | `Wrong_page_size size -> Format.fprintf h "Wrong_page_size %i" size
    | `Wrong_disk_size size ->
      Format.fprintf h "Wrong_disk_size %s" (Int64.to_string size)
    | `Wrong_checksum_algorithm (name, byte_size) ->
      Format.fprintf h "Wrong_checksum_algorithm (%S, %i)" name byte_size

  module type S =
    S
      with type Disk.read_error = Block.error
       and type Disk.write_error = Block.write_error

  type t = T : (module S with type t = 'a) * 'a -> t

  let make_disk block =
    let open Lwt.Syntax in
    let+ (module A_disk) = Context.of_impl (module Block) (module Check) block in
    Ok (module Make_disk (A_disk) : S)

  exception Fs of error

  open Lwt.Syntax

  let or_fail pp s lwt =
    Lwt.map
      (function
       | Ok r -> r
       | Error err ->
         Format.printf "ERROR in %s: %a@." s pp err ;
         raise (Fs err))
      lwt

  let split_filename filename =
    List.filter (fun s -> s <> "") @@ String.split_on_char '/' filename

  let format block =
    let or_fail _ lwt =
      Lwt.map
        (function
         | Ok r -> r
         | Error err -> raise (Fs err))
        lwt
    in
    or_fail "Notafs.format"
    @@
    let open Lwt_result.Syntax in
    let* (module S) = make_disk block in
    let+ (t : S.t) = S.format () in
    T ((module S), t)

  let stats (T ((module S), _)) = Stats.snapshot S.Disk.stats

  let connect block =
    let* (module A_disk) = Context.of_impl (module Block) (module Check) block in
    let (module S) = (module Make_disk (A_disk) : S) in
    or_fail S.pp_error "Notafs.connect"
    @@
    let open Lwt_result.Syntax in
    let+ (t : S.t) = S.connect () in
    T ((module S), t)

  let flush (T ((module S), t)) = or_fail S.pp_error "Notafs.flush" @@ S.flush t
  let clear (T ((module S), _)) = or_fail S.pp_error "Notafs.clear" @@ S.clear ()
  let exists (T ((module S), t)) filename = S.exists t (split_filename filename)

  type file = File : (module S with type t = 'a and type file = 'b) * 'a * 'b -> file

  let find (T ((module S), t)) filename =
    match S.find_opt t (split_filename filename) with
    | None -> None
    | Some file -> Some (File ((module S), t, file))

  let filename (File ((module S), _, file)) = S.filename file

  let remove (T ((module S), t)) filename =
    or_fail S.pp_error "Notafs.remove" @@ S.remove t (split_filename filename)

  let touch (T ((module S), t)) filename contents =
    or_fail S.pp_error "Notafs.touch"
    @@
    let open Lwt_result.Syntax in
    let filename = split_filename filename in
    let+ file = S.touch t filename contents in
    File ((module S), t, file)

  let rename (T ((module S), t)) ~src ~dst =
    if debug then Format.printf "Notafs.rename@." ;
    let src = split_filename src in
    let dst = split_filename dst in
    or_fail S.pp_error "Notafs.rename" @@ S.rename t ~src ~dst

  let append_substring (File ((module S), t, file)) str ~off ~len =
    or_fail S.pp_error "Notafs.append_substring"
    @@ S.append_substring t file str ~off ~len

  let blit_to_bytes (File ((module S), t, file)) ~off ~len bytes =
    or_fail S.pp_error "Notafs.blit_to_bytes" @@ S.blit_to_bytes t file ~off ~len bytes

  let blit_from_string (File ((module S), t, file)) ~off ~len string =
    or_fail S.pp_error "Notafs.blit_from_string"
    @@ S.blit_from_string t file ~off ~len string

  let size (File ((module S), _, file)) =
    or_fail S.pp_error "Notafs.size"
    @@
    let open Lwt_result.Syntax in
    let+ r = S.size file in
    if debug then Format.printf "Notafs.size: %i@." r ;
    r
end

let get_config (type a) (module Block : Mirage_block.S with type t = a) (block : a) =
  let open Lwt.Syntax in
  let* (module A_disk) =
    Context.of_impl (module Block) (module Checksum.No_checksum) block
  in
  let (module H) =
    (module Header.Make (A_disk) : Header.CONFIG with type error = A_disk.error)
  in
  let+ result = H.load_config () in
  match result with
  | Ok config -> Ok config
  | Error `Disk_not_formatted -> Error `Disk_not_formatted
  | Error _ -> failwith "error"

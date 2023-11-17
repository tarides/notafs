module Int63 = Optint.Int63

module Make (Clock : Mirage_clock.MCLOCK) (B : Notafs.DISK) = struct
  module Fs = Notafs.Make (B)

  module IO = struct
    open Lwt.Syntax

    type t = Fs.file

    let fs = ref None

    let init block =
      let+ filesystem = Fs.of_block block in
      fs := Some filesystem

    let fs () =
      match !fs with
      | Some fs -> fs
      | None -> failwith "expected fs?"

    let exists filename = Fs.exists (fs ()) filename = Some `Value

    module Header_raw = struct
      let max_fan_index = 0
      let max_fan_size = Int63.encoded_size
      let version_index = max_fan_index + max_fan_size
      let version_size = Int63.encoded_size
      let generation_index = version_index + version_size
      let generation_size = Int63.encoded_size
      let fan_size_index = generation_index + generation_size
      let fan_size_size = Int63.encoded_size
      let size = fan_size_index + fan_size_size

      let encode_int63 n =
        let buf = Bytes.create Int63.encoded_size in
        Int63.encode buf ~off:0 n ;
        Bytes.unsafe_to_string buf

      let decode_int63 buf = Int63.decode ~off:0 buf

      let read file off len =
        let bytes = Bytes.create len in
        let q = Lwt_direct.direct (fun () -> Fs.blit_to_bytes file ~off ~len bytes) in
        assert (q = len) ;
        Bytes.to_string bytes

      let get_max_fan str = decode_int63 (read str max_fan_index max_fan_size)
      let get_version str = decode_int63 (read str version_index version_size)
      let get_generation str = decode_int63 (read str generation_index generation_size)

      let get_fan_size str =
        Int63.to_int (decode_int63 (read str fan_size_index fan_size_size))

      let get_fan file =
        let len = get_fan_size file in
        read file size len

      let set_max_fan file v =
        Fs.blit_from_string file ~off:max_fan_index ~len:max_fan_size (encode_int63 v)

      let set_version file v =
        Fs.blit_from_string file ~off:version_index ~len:version_size (encode_int63 v)

      let set_generation file v =
        Fs.blit_from_string
          file
          ~off:generation_index
          ~len:generation_size
          (encode_int63 v)

      let set_fan_size file v =
        let v = Int63.of_int v in
        Fs.blit_from_string file ~off:fan_size_index ~len:fan_size_size (encode_int63 v)

      let set_fan file str =
        let len = String.length str in
        let out = encode_int63 (Int63.of_int len) ^ str in
        assert (fan_size_size + len = String.length out) ;
        Fs.blit_from_string file ~off:fan_size_index ~len:(String.length out) out

      let do_make_string ~offset:_ ~version ~generation ~max_fan ~fan =
        let fan_size = String.length fan in
        encode_int63 max_fan
        ^ encode_int63 version
        ^ encode_int63 generation
        ^ encode_int63 (Int63.of_int fan_size)
        ^ fan
        ^ String.make (Int63.to_int max_fan - fan_size) '~'

      let header_size _rope = size + 0 (*  Int63.to_int (get_max_fan rope) *)
    end

    let v ?flush_callback:_ ~fresh ~generation ~fan_size filename =
      let ex = exists filename in
      if not (fresh || not ex)
      then (
        match Fs.find (fs ()) filename with
        | None -> Printf.ksprintf failwith "Not_found: Index.v %S" filename
        | Some file -> file)
      else begin
        let header =
          Header_raw.do_make_string
            ~version:Int63.zero
            ~offset:Int63.zero
            ~generation
            ~max_fan:fan_size
            ~fan:""
        in
        if ex then Lwt_direct.direct (fun () -> Fs.remove (fs ()) filename) ;
        Lwt_direct.direct (fun () -> Fs.touch (fs ()) filename header)
      end

    let v_readonly filename =
      match Fs.find (fs ()) filename with
      | None -> Error `No_file_on_disk
      | Some file -> Ok file

    let size_header file = Header_raw.header_size file

    let offset filename =
      let size = Lwt_direct.direct (fun () -> Fs.size filename) in
      let hs = size_header filename in
      Int63.of_int (size - hs)

    let read file ~off ~len bytes =
      let off = Int63.to_int off in
      let header_size = size_header file in
      let q =
        Lwt_direct.direct (fun () ->
          Fs.blit_to_bytes file ~off:(off + header_size) ~len bytes)
      in
      assert (q = len) ;
      q

    let clear ~generation ?hook:_ ~reopen file =
      let reopen =
        if not reopen
        then None
        else begin
          let len = Header_raw.header_size file in
          let bytes = Bytes.create len in
          let q = Lwt_direct.direct (fun () -> Fs.blit_to_bytes file ~off:0 ~len bytes) in
          assert (q = len) ;
          Some (Bytes.to_string bytes)
        end
      in
      Lwt_direct.direct
      @@ fun () ->
      match reopen with
      | None ->
        let* () = Fs.remove (fs ()) (Fs.filename file) in
        Lwt.return ()
      | Some header ->
        let* () = Fs.replace (fs ()) (Fs.filename file) header in
        Header_raw.set_generation file generation

    let flush ?no_callback:_ ?with_fsync:_ _ = ()
    let get_generation filename = Header_raw.get_generation filename
    let set_fanout file v = Lwt_direct.direct (fun () -> Header_raw.set_fan file v)
    let get_fanout file = Header_raw.get_fan file
    let get_fanout_size file = Int63.of_int (Header_raw.get_fan_size file)

    let rename ~src ~dst =
      Lwt_direct.direct (fun () ->
        Fs.rename (fs ()) ~src:(Fs.filename src) ~dst:(Fs.filename dst))

    let append_substring file str ~off ~len =
      Lwt_direct.direct (fun () -> Fs.append_substring file str ~off ~len)

    let append filename str =
      append_substring filename str ~off:0 ~len:(String.length str)

    let close _filename = ()

    module Lock = struct
      type t = string

      let lock str = str
      let unlock _lc = ()
      let pp_dump _str = None
    end

    module Header = struct
      type header =
        { offset : Int63.t
        ; generation : Int63.t
        }

      let set _file _header = failwith "Header.set" (* unused? *)
      let get file = { offset = offset file; generation = get_generation file }
    end

    let size _file = failwith "Header.size" (* unused? *)
  end

  module Semaphore = struct
    type t = Lwt_mutex.t

    let make bool =
      let t = Lwt_mutex.create () in
      if not bool then Lwt_direct.direct (fun () -> Lwt_mutex.lock t) ;
      t

    let acquire _str t = Lwt_direct.direct (fun () -> Lwt_mutex.lock t)
    let release t = Lwt_mutex.unlock t

    let with_acquire _str t fn =
      Lwt_direct.direct (fun () ->
        Lwt_mutex.with_lock t (fun () -> Lwt_direct.indirect fn))

    let is_held t = Lwt_mutex.is_locked t
  end

  module Clock = struct
    type counter = int64

    let counter () = Clock.elapsed_ns ()

    let count t =
      let now = Clock.elapsed_ns () in
      Mtime.Span.of_uint64_ns (Int64.sub now t)

    let start = counter ()
    let elapsed () = count start

    let now () =
      let now = Clock.elapsed_ns () in
      Mtime.of_uint64_ns (Int64.sub now start)
  end

  (* TODO: this triggers a race condition in index merge (?)

     module Thread_lwt = struct
     type 'a t = ('a, [ `Async_exn of exn ]) result Lwt.t

     let async fn =
     Lwt.catch
     (fun () ->
     let open Lwt.Syntax in
     let+ x = Lwt_direct.indirect fn in
     Ok x)
     (fun exn -> Lwt.return (Error (`Async_exn exn)))

     let await t = Lwt_direct.direct (fun () -> t)
     let return x = Lwt.return (Ok x)
     let yield () = Lwt_direct.direct (fun () -> Lwt.pause ())
     end
  *)

  module Thread = struct
    type 'a t = ('a, [ `Async_exn of exn ]) result

    let async fn =
      Lwt_direct.direct (fun () ->
        Lwt.catch
          (fun () ->
            let open Lwt.Syntax in
            let+ x = Lwt_direct.indirect fn in
            Ok x)
          (fun exn -> Lwt.return (Error (`Async_exn exn))))

    let await t = t
    let return x = Ok x
    let yield () = Lwt_direct.direct Lwt.pause
  end

  module Progress = Progress_engine.Make (struct
      module Clock = Clock

      module Terminal_width = struct
        let set_changed_callback _ = ()
        let get () = None
      end
    end)

  module Fmt_tty = struct
    let setup_std_outputs ?style_renderer ?(utf_8 = false) () =
      let config formatter =
        Option.iter (Fmt.set_style_renderer formatter) style_renderer ;
        Fmt.set_utf_8 formatter utf_8
      in
      config Format.std_formatter ;
      config Format.err_formatter
  end
end

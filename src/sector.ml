module Make (B : Context.A_DISK) : sig
  type id = Int64.t

  val id_size : int
  val id_t : id Repr.t
  val null_id : id
  val is_null_id : id -> bool

  type ptr

  val ptr_t : ptr Repr.t
  val null_ptr : ptr
  val is_null_ptr : ptr -> bool
  val ptr_size : int

  type t

  val force_id : t -> id
  val to_ptr : t -> ptr

  (* val unsafe_set_id : t -> id -> unit *)
  type loc

  val root_loc : Int64.t -> loc
  val create : ?at:loc -> unit -> t
  val load_root : id -> (t, [> `Read of B.read_error ]) Lwt_result.t
  val load : ptr -> (t, [> `Read of B.read_error ]) Lwt_result.t
  val write : t -> (unit, [> `Write of B.write_error ]) Lwt_result.t
  val length : t -> int
  val get_uint8 : t -> int -> int
  val set_uint8 : t -> int -> int -> unit
  val get_uint16 : t -> int -> int
  val set_uint16 : t -> int -> int -> unit
  val get_uint32 : t -> int -> int
  val set_uint32 : t -> int -> int -> unit
  val get_uint64 : t -> int -> Int64.t
  val set_uint64 : t -> int -> Int64.t -> unit
  val get_child : t -> int -> (t, [> `Read of B.read_error ]) Lwt_result.t
  val set_child : t -> int -> t -> unit
  val get_child_ptr : t -> int -> ptr
  val set_child_ptr : t -> int -> ptr -> unit
  val erase_child_ptr : t -> int -> unit
  val erase_region : t -> off:int -> len:int -> unit
  val blit_from_string : string -> int -> t -> int -> int -> unit
  val blit_to_bytes : t -> int -> bytes -> int -> int -> unit

  (* *)
  val count_new : t -> int
  val finalize : t -> id list -> t list * id list
  val drop_release : t -> unit
  val is_in_memory : t -> bool
end = struct
  module H = Hashtbl.Make (struct
      type t = int

      let equal = Int.equal
      let hash = Hashtbl.hash
    end)

  module C = B.C

  type id = Int64.t

  let id_t : id Repr.t = Repr.int64

  type loc =
    | Root of id
    | In_memory
    | At of id

  type t =
    { mutable id : loc
    ; cstruct : Cstruct.t
    ; children : t H.t
    ; mutable parent : parent
    }

  and parent =
    | Detached
    | Parent of t * int

  type ptr =
    | Disk of id * C.t
    | Mem of t

  let cs_t : C.t Repr.t =
    Repr.map
      Repr.int32
      (fun cs-> C.of_int32 cs)
      (fun cs -> C.to_int32 cs)

  let get_checksum cstruct = C.digest_bigstring (Cstruct.to_bigarray cstruct) 0 B.page_size C.default

  let ptr_t : ptr Repr.t =
    Repr.map
      Repr.(pair id_t cs_t)
      (fun (id, cs) -> Disk (id, cs))
      (function
       | Disk (id, cs) -> id, cs
       | _ -> invalid_arg "Sector.ptr_t: serialize Mem")

  let to_ptr t =
    match t.id with
    | Root id | At id ->
      let cs = get_checksum t.cstruct in
      Disk (id, cs)
    | In_memory -> invalid_arg "Sector.to_ptr: not allocated"

  let null_id = Int64.zero
  let null_cs = C.default
  let id_size = 8
  let cs_size = 4
  let ptr_size = id_size + cs_size
  let is_null_id id = Int64.equal null_id id
  let is_null_cs cs = C.equal null_cs cs
  let null_ptr = Disk (null_id, null_cs)

  let is_null_ptr = function
    | Disk (id, cs) -> is_null_id id && is_null_cs cs
    | Mem _ -> false

  let root_loc i = Root i

  let create ?(at = In_memory) () =
    { id = at
    ; cstruct = Cstruct.create B.page_size
    ; children = H.create 4
    ; parent = Detached
    }

  let is_in_memory t =
    match t.id with
    | At _ -> false
    | _ -> true

  let set_child_ptr t offset = function
    | Disk (id, cs) ->
      Cstruct.HE.set_uint64 t.cstruct offset id;
      Cstruct.HE.set_uint32 t.cstruct (offset + id_size) (C.to_int32 cs)
    | Mem child ->
      assert (is_in_memory t) ;
      Cstruct.HE.set_uint64 t.cstruct offset null_id ;
      Cstruct.HE.set_uint32 t.cstruct (offset + id_size) (C.to_int32 null_cs) ;
      H.replace t.children offset child ;
      assert (H.mem t.children offset)

  let ptr_of_t t =
    match t.id with
    | Root id | At id ->
      let cs = get_checksum t.cstruct in
      Disk (id, cs)
    | In_memory -> Mem t

  let set_child t offset child =
    begin
      match child.parent with
      | Parent (t', _) when t == t' -> ()
      | Detached -> ()
      | _ -> failwith "set_child: would lose parent"
    end ;
    child.parent <- Parent (t, offset) ;
    set_child_ptr t offset (ptr_of_t child)

  open Lwt_result.Syntax

  let release t =
    match t.id with
    | In_memory | Root _ -> ()
    | At id ->
      B.discard id ;
      t.id <- In_memory

  let rec release_parent t =
    match t.parent with
    | Detached -> ()
    | Parent (parent, offset) ->
      release parent ;
      set_child parent offset t ;
      release_parent parent

  let release t =
    release t ;
    release_parent t

  let drop_release t =
    match t.id with
    | In_memory -> ()
    | Root _ -> invalid_arg "Sector.drop_release: Root"
    | At id ->
      B.discard id ;
      t.id <- In_memory

  let set_child_ptr t offset =
    release t ;
    set_child_ptr t offset

  let erase_child_ptr t offset =
    release t ;
    Cstruct.HE.set_uint64 t.cstruct offset null_id ;
    Cstruct.HE.set_uint32 t.cstruct (offset + id_size) (C.to_int32 null_cs) ;
    try H.remove t.children offset with
    | Not_found -> ()

  let erase_region t ~off ~len =
    release t ;
    Cstruct.blit t.cstruct (off + len) t.cstruct off (Cstruct.length t.cstruct - off - len) ;
    for i = Cstruct.length t.cstruct - len to Cstruct.length t.cstruct - 1 do
      Cstruct.set_uint8 t.cstruct i 0
    done ;
    for i = off to off + len - 1 do
      match H.find_opt t.children i with
      | None -> ()
      | Some child ->
        assert (is_in_memory child) ;
        H.remove t.children i
    done ;
    for i = off + len to Cstruct.length t.cstruct - 1 do
      match H.find_opt t.children i with
      | None -> ()
      | Some child ->
        let j = i - len in
        H.remove t.children i ;
        H.replace t.children j child ;
        child.parent <- Parent (t, j)
    done

  let length t = Cstruct.length t.cstruct
  let get_uint8 t offset = Cstruct.get_uint8 t.cstruct offset
  let get_uint16 t offset = Cstruct.HE.get_uint16 t.cstruct offset
  let get_uint32 t offset = Int32.to_int @@ Cstruct.HE.get_uint32 t.cstruct offset
  let get_uint64 t offset = Cstruct.HE.get_uint64 t.cstruct offset

  let set_uint8 t offset v =
    release t ;
    Cstruct.set_uint8 t.cstruct offset v

  let set_uint16 t offset v =
    release t ;
    Cstruct.HE.set_uint16 t.cstruct offset v

  let set_uint32 t offset v =
    release t ;
    Cstruct.HE.set_uint32 t.cstruct offset (Int32.of_int v)

  let set_uint64 t offset v =
    release t ;
    Cstruct.HE.set_uint64 t.cstruct offset v

  let get_child_ptr t offset =
    try Mem (H.find t.children offset) with
    | Not_found ->
      let id = Cstruct.HE.get_uint64 t.cstruct offset in
      let cs = C.of_int32 (Cstruct.HE.get_uint32 t.cstruct (offset + id_size)) in
      Disk (id, cs)

  let read id =
    let cstruct = Cstruct.create B.page_size in
    let+ () = B.read id cstruct in
    cstruct

  let load_root id =
    let+ cstruct = read id in
    { id = Root id; cstruct; children = H.create 4; parent = Detached }

  let load = function
    | Disk (id, cs) ->
      let+ cstruct = read id in
      let cs' = get_checksum cstruct in
      assert (C.equal cs cs');
      { id = At id; cstruct; children = H.create 4; parent = Detached }
    | Mem t -> Lwt_result.return t

  let get_child t offset =
    match get_child_ptr t offset with
    | Mem child -> Lwt_result.return child
    | Disk (id, cs) ->
      let+ cstruct = read id in
      let cs' = get_checksum cstruct in
      assert (C.equal cs cs');
      { id = At id; cstruct; children = H.create 4; parent = Parent (t, offset) }

  let set_child t offset child =
    release t ;
    set_child t offset child

  let rec count_new t acc =
    match t.id with
    | Root _ | At _ -> acc
    | In_memory -> H.fold (fun _ child acc -> count_new child acc) t.children (acc + 1)

  let count_new t = count_new t 0

  let rec finalize t ids acc =
    match t.id, ids with
    | In_memory, id :: ids ->
      t.id <- At id ;
      let ids, acc =
        H.fold
          (fun offset child (ids, acc) ->
            match child.id with
            | Root id | At id ->
              let prev = Cstruct.HE.get_uint64 t.cstruct offset in
              assert (Int64.equal prev id) ;
              ids, acc
            | In_memory ->
              let prev = Cstruct.HE.get_uint64 t.cstruct offset in
              assert (Int64.equal prev null_id) ;
              let child_id, child_cs, acc, ids = finalize child ids acc in
              Cstruct.HE.set_uint64 t.cstruct offset child_id ;
              Cstruct.HE.set_uint32 t.cstruct (offset + id_size) (C.to_int32 child_cs);
              ids, acc)
          t.children
          (ids, acc)
      in
      H.clear t.children ;
      let cs = get_checksum t.cstruct in
      id, cs, t :: acc, ids
    | (Root id | At id), _ ->
      let cs = get_checksum t.cstruct in
      id, cs, acc, ids
    | _, [] -> invalid_arg "Sector.finalize: empty list"

  let finalize t ids =
    let e = count_new t in
    let _, _, ts, ids' = finalize t ids [] in
    assert (List.length ids' + e = List.length ids) ;
    ts, ids'

  let force_id t =
    match t.id with
    | At id | Root id -> id
    | In_memory -> failwith "Sector.force_id"

  let blit_from_string str i t j n =
    release t ;
    Cstruct.blit_from_string str i t.cstruct j n

  let blit_to_bytes t i bytes j n =
    if n = 0 then () else Cstruct.blit_to_bytes t.cstruct i bytes j n

  let write t =
    let id = force_id t in
    B.write id t.cstruct
end

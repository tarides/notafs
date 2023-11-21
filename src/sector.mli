module Make (B : Context.A_DISK) : sig
  type 'a r := ('a, B.error) Lwt_result.t
  type id = B.Id.t [@@deriving repr]

  val id_size : int

  type ptr [@@deriving repr]

  val null_ptr : ptr
  val is_null_ptr : ptr -> bool
  val ptr_size : int

  type t

  val force_id : t -> id
  val to_ptr : t -> ptr

  type loc

  val root_loc : id -> loc
  val create : ?at:loc -> unit -> t r
  val load_root : ?check:bool -> id -> t r
  val load : ptr -> t r
  val verify_checksum : t -> unit r

  (* *)
  val write_root : t -> unit r
  val to_write : t -> (id * Cstruct.t) r
  val count_new : t -> int r
  val is_in_memory : t -> bool
  val finalize : t -> id list -> ((id * Cstruct.t) list * id list) r
  val free : t -> unit

  (* *)
  val get_uint8 : t -> int -> int r
  val set_uint8 : t -> int -> int -> unit r
  val get_uint16 : t -> int -> int r
  val set_uint16 : t -> int -> int -> unit r
  val get_uint32 : t -> int -> int r
  val set_uint32 : t -> int -> int -> unit r
  val get_uint64 : t -> int -> Int64.t r
  val set_uint64 : t -> int -> Int64.t -> unit r
  val read_id : t -> int -> id r
  val write_id : t -> int -> id -> unit r
  val get_child : t -> int -> t r
  val set_child : t -> int -> t -> unit r
  val get_child_ptr : t -> int -> ptr r
  val set_child_ptr : t -> int -> ptr -> unit r
  val erase_region : t -> off:int -> len:int -> unit r
  val detach_region : t -> off:int -> len:int -> unit r
  val blit_from_string : string -> int -> t -> int -> int -> unit r
  val blit_to_bytes : t -> int -> bytes -> int -> int -> unit r
end

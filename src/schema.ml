module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)

  type 'a field =
    { get : Sector.t -> ('a, B.error) Lwt_result.t
    ; set : Sector.t -> 'a -> (unit, B.error) Lwt_result.t
    }
  (* let bifield_map f g t = { get = (fun s -> f (t.get s)); set = (fun s x -> t.set s (g x)) } *)

  let bifield_pair x y =
    let open Lwt_result.Syntax in
    { get =
        (fun s ->
          let* a = x.get s in
          let+ b = y.get s in
          a, b)
    ; set =
        (fun s (a, b) ->
          let* () = x.set s a in
          let+ () = y.set s b in
          ())
    }

  module Infix = struct
    let ( .@() ) t i = i.get t
    let ( .@()<- ) t i v = i.set t v
  end

  type 'a t = max_size:int -> int -> int * 'a

  let define t =
    let max_size = B.page_size in
    let ofs, x = t ~max_size 0 in
    if ofs > max_size then failwith "Schema size is larger than Cstruct max size" ;
    x

  let map fn t ~max_size ofs =
    let ofs, a = t ~max_size ofs in
    ofs, fn a

  let map2 fn x y ~max_size ofs =
    let ofs, x = x ~max_size ofs in
    let ofs, y = y ~max_size ofs in
    ofs, fn x y

  let pair x y ~max_size ofs =
    let ofs, x = x ~max_size ofs in
    let ofs, y = y ~max_size ofs in
    ofs, (x, y)

  let bind fn t ~max_size ofs =
    let ofs, x = t ~max_size ofs in
    fn x ~max_size ofs

  module Syntax = struct
    let ( let+ ) t fn = map fn t
    let ( and+ ) = pair
    let ( let* ) t fn = bind fn t
    let ( let| ) t fn = map fn t

    let ( and| ) a b ~max_size ofs =
      let a_ofs, a = a ~max_size ofs
      and b_ofs, b = b ~max_size ofs in
      max a_ofs b_ofs, (a, b)
  end

  let field_pair x y ~max_size ofs =
    let ofs, x = x ~max_size ofs in
    let ofs, y = y ~max_size ofs in
    ofs, bifield_pair x y

  let make size get set : 'a t =
    fun ~max_size:_ ofs ->
    ofs + size, { get = (fun cs -> get cs ofs); set = (fun cs v -> set cs ofs v) }

  let char = make 1 Sector.get_uint8 Sector.set_uint8
  let uint8 = make 1 Sector.get_uint8 Sector.set_uint8
  let uint16 = make 2 Sector.get_uint16 Sector.set_uint16
  let uint32 = make 4 Sector.get_uint32 Sector.set_uint32
  let uint64 = make 8 Sector.get_uint64 Sector.set_uint64

  type 'a dyn_array =
    { length : int field
    ; max_length : int
    ; location : int
    ; size_of_thing : int
    ; thing : 'a t
    }

  let size_of ~max_size thing =
    let size, _ = thing ~max_size 0 in
    size

  let array thing : 'a dyn_array t =
    fun ~max_size ofs ->
    let ofs, length = uint16 ~max_size ofs in
    let rest = max_size - ofs in
    let size_of_thing = size_of ~max_size:rest thing in
    let max_length = rest / size_of_thing in
    let end_ofs = ofs + (max_length * size_of_thing) in
    end_ofs, { length; max_length; location = ofs; size_of_thing; thing }

  let string = array char

  let nth array i =
    let _, t = array.thing ~max_size:0 (array.location + (array.size_of_thing * i)) in
    t

  type child = Sector.t field

  let child : child t = make Sector.ptr_size Sector.get_child Sector.set_child

  type id = Sector.id field

  let id : id t = make Sector.id_size Sector.read_id Sector.write_id

  type ptr = Sector.ptr field

  let ptr : ptr t = make Sector.ptr_size Sector.get_child_ptr Sector.set_child_ptr
end

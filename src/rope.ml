module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Schema = Schema.Make (B)
  open Lwt_result.Syntax

  type t = Sector.t

  let count_new = Sector.count_new
  let height_index = 0
  let nb_children_index = 1
  let header_size = 3
  let get_height t = Sector.get_uint8 t height_index
  let set_height t v = Sector.set_uint8 t height_index v
  let get_nb_children t = Sector.get_uint16 t nb_children_index
  let set_nb_children t v = Sector.set_uint16 t nb_children_index v
  let key_size = 4
  let child_size = key_size + Sector.ptr_size
  let key_index i = header_size + (child_size * i)
  let child_index i = key_index i + key_size
  let max_children t = (Sector.length t - header_size) / child_size
  let get_key t i = Sector.get_uint32 t (key_index i)
  let get_child t i = Sector.get_child t (child_index i)
  let set_child t i v = Sector.set_child t (child_index i) v

  type append_result =
    | Ok
    | Rest of int

  module Leaf = struct
    type nonrec t = t

    let max_length t = Sector.length t - header_size
    let get_length = get_nb_children
    let set_length = set_nb_children

    let create () =
      let t = Sector.create () in
      set_height t 0 ;
      set_length t 0 ;
      t

    let append t (str, i) =
      let max_len = max_length t in
      let len = get_length t in
      let capacity = max_len - len in
      if capacity <= 0
      then t, Rest i
      else begin
        let rest = String.length str - i in
        let quantity = min rest capacity in
        Sector.blit_from_string str i t (header_size + len) quantity ;
        set_length t (len + quantity) ;
        let res = if quantity < rest then Rest (i + quantity) else Ok in
        t, res
      end

    let blit_to_bytes t offs bytes i quantity =
      let len = get_length t in
      let quantity = min quantity (len - offs) in
      if quantity = 0
      then 0
      else begin
        assert (quantity > 0) ;
        assert (offs >= 0) ;
        assert (offs < len) ;
        assert (offs + quantity <= len) ;
        Sector.blit_to_bytes t (offs + header_size) bytes i quantity ;
        quantity
      end

    let blit_from_string t offs str i quantity =
      let len = get_length t in
      assert (quantity > 0) ;
      assert (offs >= 0) ;
      assert (offs < len) ;
      assert (offs + quantity <= len) ;
      Sector.blit_from_string str i t (offs + header_size) quantity
  end

  let size t =
    let height = get_height t in
    if height = 0
    then Leaf.get_length t
    else (
      let nb = get_nb_children t in
      if nb = 0 then 0 else get_key t (nb - 1))

  let set_key t i v = Sector.set_uint32 t (key_index i) v

  let create () =
    let t = Sector.create () in
    set_height t 0 ;
    set_nb_children t 0 ;
    t

  let load ptr =
    if Sector.is_null_ptr ptr then Lwt_result.return (create ()) else Sector.load ptr

  let rec do_append t (str, i) =
    assert (i < String.length str) ;
    let height = get_height t in
    if height = 0
    then Lwt_result.return (Leaf.append t (str, i))
    else begin
      let len = get_nb_children t in
      assert (len > 0) ;
      let last_index = len - 1 in
      let* last_child = get_child t last_index in
      let last_child_len = size last_child in
      let check =
        (try get_key t (last_index - 1) with
         | _ -> 0)
        + last_child_len
        = get_key t last_index
      in
      if not check then failwith "Rope.check" ;
      let* last_child', res = do_append last_child (str, i) in
      assert (last_child' == last_child) ;
      match res with
      | Ok ->
        set_key t last_index (get_key t last_index + String.length str - i) ;
        Lwt_result.return (t, Ok)
      | Rest i' when i = i' ->
        (* no progress, child is full *)
        if len >= max_children t
        then Lwt_result.return (t, Rest i)
        else begin
          let leaf = Leaf.create () in
          set_nb_children t (len + 1) ;
          set_child t len leaf ;
          set_key t len (get_key t last_index) ;
          do_append t (str, i)
        end
      | Rest i' ->
        set_key t last_index (get_key t last_index + i' - i) ;
        do_append t (str, i')
    end

  let rec append_from t (str, i) =
    let* t, res = do_append t (str, i) in
    match res with
    | Ok -> Lwt_result.return t
    | Rest i ->
      let root = create () in
      set_height root (get_height t + 1) ;
      set_nb_children root 1 ;
      let key = size t in
      set_child root 0 t ;
      set_key root 0 key ;
      append_from root (str, i)

  let append t str = append_from t (str, 0)

  let rec blit_to_bytes ~depth t i bytes j n =
    let height = get_height t in
    assert (n >= 0) ;
    if n = 0
    then Lwt_result.return 0
    else if height = 0
    then Lwt_result.return (Leaf.blit_to_bytes t i bytes j n)
    else begin
      let requested_read_length = n in
      let j = ref j in
      let n = ref n in
      let rec go k =
        if k >= get_nb_children t || !n <= 0
        then Lwt_result.return ()
        else begin
          let offs_stop = get_key t k in
          let* () =
            if i >= offs_stop
            then Lwt_result.return ()
            else begin
              let offs_start = if k = 0 then 0 else get_key t (k - 1) in
              let len = offs_stop - offs_start in
              assert (len >= 0) ;
              let sub_i = max 0 (i - offs_start) in
              let quantity = min !n (len - sub_i) in
              let* child = get_child t k in
              let+ q = blit_to_bytes ~depth:(depth + 1) child sub_i bytes !j quantity in
              assert (q = quantity) ;
              j := !j + quantity ;
              n := !n - quantity
            end
          in
          go (k + 1)
        end
      in
      let+ () = go 0 in
      requested_read_length - !n
    end

  let blit_to_bytes t i bytes j n = blit_to_bytes ~depth:0 t i bytes j n

  let rec blit_from_string t i bytes j n =
    let height = get_height t in
    if height = 0
    then begin
      Leaf.blit_from_string t i bytes j n ;
      Lwt_result.return ()
    end
    else begin
      let j = ref j in
      let n = ref n in
      let rec go k =
        if k >= get_nb_children t || !n <= 0
        then Lwt_result.return ()
        else begin
          let offs_stop = get_key t k in
          let* () =
            if i >= offs_stop
            then Lwt_result.return ()
            else begin
              let offs_start = if k = 0 then 0 else get_key t (k - 1) in
              let len = offs_stop - offs_start in
              assert (len >= 0) ;
              let sub_i = max 0 (i - offs_start) in
              let quantity = min !n (len - sub_i) in
              let* child = get_child t k in
              let+ () = blit_from_string child sub_i bytes !j quantity in
              j := !j + quantity ;
              n := !n - quantity
            end
          in
          go (k + 1)
        end
      in
      let+ () = go 0 in
      assert (!n = 0)
    end

  let blit_from_string t i bytes j n =
    let len = size t in
    if i + n <= len
    then
      let+ () = blit_from_string t i bytes j n in
      t
    else
      let* rest =
        if i < len
        then (
          let m = len - i in
          let+ () = blit_from_string t i bytes j m in
          String.sub bytes (j + m) (n - m))
        else Lwt_result.return bytes
      in
      append t rest

  let to_string rope =
    let len = size rope in
    if len = 0
    then Lwt_result.return ""
    else begin
      let bytes = Bytes.create len in
      let+ q = blit_to_bytes rope 0 bytes 0 len in
      assert (q = len) ;
      Bytes.to_string bytes
    end

  let of_string str =
    let t = create () in
    if str = "" then Lwt_result.return t else append t str

  let rec free t =
    Sector.drop_release t ;
    let height = get_height t in
    if height = 0
    then Lwt_result.return ()
    else begin
      let n = get_nb_children t in
      let rec go i =
        if i >= n
        then Lwt_result.return ()
        else
          let* child = get_child t i in
          let* () = free child in
          go (i + 1)
      in
      go 0
    end
end

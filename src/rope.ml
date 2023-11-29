module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Schema = Schema.Make (B)
  open Lwt_result.Syntax

  type t = Sector.t

  let count_new = Sector.count_new
  let height_index = 0
  let nb_children_index = 2
  let header_size = 4
  let get_height t = Sector.get_uint16 t height_index
  let set_height t v = Sector.set_uint16 t height_index v
  let get_nb_children t = Sector.get_uint16 t nb_children_index
  let set_nb_children t v = Sector.set_uint16 t nb_children_index v
  let key_size = 4
  let child_size = key_size + Sector.ptr_size
  let key_index i = header_size + (child_size * i)
  let child_index i = key_index i + key_size
  let max_children = (B.page_size - header_size) / child_size
  let get_key t i = Sector.get_uint32 t (key_index i)
  let set_key t i v = Sector.set_uint32 t (key_index i) v
  let get_child t i = Sector.get_child t (child_index i)
  let set_child t i v = Sector.set_child t (child_index i) v
  let get_child_ptr t i = Sector.get_child_ptr t (child_index i)

  type append_result =
    | Ok
    | Rest of int

  module Leaf = struct
    type nonrec t = t

    let max_length = B.page_size - header_size
    let get_length = get_nb_children
    let set_length = set_nb_children

    let create () =
      let* t = Sector.create () in
      let* () = set_height t 0 in
      let+ () = set_length t 0 in
      t

    let append t (str, i, str_len) =
      let* len = get_length t in
      let capacity = max_length - len in
      if capacity <= 0
      then Lwt_result.return (t, Rest i)
      else begin
        let rest = str_len - i in
        let quantity = min rest capacity in
        let* () = Sector.blit_from_string str i t (header_size + len) quantity in
        let+ () = set_length t (len + quantity) in
        let res = if quantity < rest then Rest (i + quantity) else Ok in
        t, res
      end

    let blit_to_bytes t offs bytes i quantity =
      let* len = get_length t in
      let quantity = min quantity (len - offs) in
      if quantity = 0
      then Lwt_result.return 0
      else begin
        assert (quantity > 0) ;
        assert (offs >= 0) ;
        assert (offs < len) ;
        assert (offs + quantity <= len) ;
        assert (i >= 0) ;
        assert (i + quantity <= Bytes.length bytes) ;
        let+ () = Sector.blit_to_bytes t (offs + header_size) bytes i quantity in
        quantity
      end

    let blit_from_string t offs str i quantity =
      let* len = get_length t in
      assert (quantity > 0) ;
      assert (offs >= 0) ;
      assert (offs < len) ;
      assert (offs + quantity <= len) ;
      Sector.blit_from_string str i t (offs + header_size) quantity
  end

  let size t =
    let* height = get_height t in
    if height = 0
    then Leaf.get_length t
    else
      let* nb = get_nb_children t in
      if nb = 0 then Lwt_result.return 0 else get_key t (nb - 1)

  let create () =
    let* t = Sector.create () in
    let* () = set_height t 0 in
    let+ () = set_nb_children t 0 in
    t

  let load ptr = if Sector.is_null_ptr ptr then create () else Sector.load ptr

  let rec verify_checksum t =
    let* () = Sector.verify_checksum t in
    let* height = get_height t in
    if height > 0
    then
      let* nb_children = get_nb_children t in
      let rec check_child i =
        if i > nb_children - 1
        then Lwt_result.return ()
        else begin
          let* t = get_child t i in
          let* () = verify_checksum t in
          check_child (i + 1)
        end
      in
      check_child 0
    else Lwt_result.return ()

  let rec do_append t (str, i, str_len) =
    assert (i < str_len) ;
    let* height = get_height t in
    if height = 0
    then Leaf.append t (str, i, str_len)
    else begin
      let* len = get_nb_children t in
      assert (len > 0) ;
      let last_index = len - 1 in
      let* last_child = get_child t last_index in
      let* last_child', res = do_append last_child (str, i, str_len) in
      assert (last_child' == last_child) ;
      match res with
      | Ok ->
        let* key_last_index = get_key t last_index in
        let* () = set_key t last_index (key_last_index + str_len - i) in
        Lwt_result.return (t, Ok)
      | Rest i' when i = i' ->
        (* no progress, child is full *)
        if len >= max_children
        then begin
          let* retry = compact t in
          if retry then do_append t (str, i, str_len) else Lwt_result.return (t, Rest i)
        end
        else begin
          let* leaf = Leaf.create () in
          let* () = set_nb_children t (len + 1) in
          let* () = set_child t len leaf in
          let* key_last_index = get_key t last_index in
          let* () = set_key t len key_last_index in
          do_append t (str, i, str_len)
        end
      | Rest i' ->
        let* key_last_index = get_key t last_index in
        let* () = set_key t last_index (key_last_index + i' - i) in
        do_append t (str, i', str_len)
    end

  and compact t =
    let* height = get_height t in
    assert (height > 0) ;
    if height = 1
    then Lwt_result.return false
    else begin
      let* len = get_nb_children t in
      assert (len = max_children) ;
      let last_index = len - 1 in
      let* last_child = get_child t last_index in
      let* child_height = get_height last_child in
      if height = child_height + 1
      then Lwt_result.return false
      else begin
        let rec go i nb acc =
          if i < 0
          then Lwt_result.return (nb, acc)
          else begin
            let* child = get_child t i in
            let* h = get_height child in
            assert (h >= child_height) ;
            if h > child_height
            then Lwt_result.return (nb, acc)
            else go (i - 1) (nb + 1) (child :: acc)
          end
        in
        let* nb, children = go (last_index - 1) 1 [ last_child ] in
        let off = key_index (len - nb) in
        let* () = Sector.detach_region t ~off ~len:(nb * child_size) in
        let* () = set_nb_children t (len - nb + 1) in
        let* new_parent = create () in
        let* () = set_height new_parent (child_height + 1) in
        let* () = set_child t (len - nb) new_parent in
        let* () = set_nb_children new_parent nb in
        let rec go i at = function
          | [] -> Lwt_result.return ()
          | c :: cs ->
            let* s = size c in
            let at = at + s in
            let* () = set_key new_parent i at in
            let* () = set_child new_parent i c in
            go (i + 1) at cs
        in
        let* () = go 0 0 children in
        let* prev_at =
          if len - nb = 0 then Lwt_result.return 0 else get_key t (len - nb - 1)
        in
        let* s = size new_parent in
        let* () = set_key t (len - nb) (prev_at + s) in
        Lwt_result.return true
      end
    end

  let rec append_from t (str, i, str_len) =
    let* t', res = do_append t (str, i, str_len) in
    assert (t == t') ;
    match res with
    | Ok -> Lwt_result.return t
    | Rest i ->
      let* root = create () in
      let* t_height = get_height t in
      let* () = set_height root (t_height + 1) in
      let* () = set_nb_children root 1 in
      let* key = size t in
      let* () = set_child root 0 t in
      let* () = set_key root 0 key in
      append_from root (str, i, str_len)

  let append t str = append_from t (str, 0, String.length str)

  let rec blit_to_bytes ~depth t i bytes j n =
    assert (i >= 0) ;
    assert (j >= 0) ;
    let* height = get_height t in
    assert (n >= 0) ;
    if n = 0
    then Lwt_result.return 0
    else if height = 0
    then Leaf.blit_to_bytes t i bytes j n
    else begin
      let requested_read_length = n in
      let* t_nb_children = get_nb_children t in
      let rec go k j n =
        if k >= t_nb_children || n <= 0
        then Lwt_result.return n
        else begin
          let* offs_stop = get_key t k in
          let* offs_start = if k = 0 then Lwt_result.return 0 else get_key t (k - 1) in
          let len = offs_stop - offs_start in
          assert (len >= 0) ;
          let sub_i = max 0 (i - offs_start) in
          let quantity = min n (len - sub_i) in
          assert (quantity <= 0 = (i >= offs_stop)) ;
          let* j, n =
            if i >= offs_stop
            then Lwt_result.return (j, n)
            else begin
              let* child = get_child t k in
              assert (sub_i >= 0) ;
              let+ q = blit_to_bytes ~depth:(depth + 1) child sub_i bytes j quantity in
              assert (q = quantity) ;
              j + quantity, n - quantity
            end
          in
          go (k + 1) j n
        end
      in
      let+ rest = go 0 j n in
      assert (rest >= 0) ;
      requested_read_length - rest
    end

  let blit_to_bytes t i bytes j n = blit_to_bytes ~depth:0 t i bytes j n

  let rec blit_from_string t i bytes j n =
    let* height = get_height t in
    if height = 0
    then Leaf.blit_from_string t i bytes j n
    else begin
      let j = ref j in
      let n = ref n in
      let* t_nb_children = get_nb_children t in
      let rec go k =
        if k >= t_nb_children || !n <= 0
        then Lwt_result.return ()
        else begin
          let* offs_stop = get_key t k in
          let* () =
            if i >= offs_stop
            then Lwt_result.return ()
            else begin
              let* offs_start =
                if k = 0 then Lwt_result.return 0 else get_key t (k - 1)
              in
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

  let blit_from_string t i str j n =
    let* len = size t in
    if i + n <= len
    then
      let+ () = blit_from_string t i str j n in
      t
    else
      let* rest =
        if i < len
        then begin
          let m = len - i in
          let+ () = blit_from_string t i str j m in
          j + m
        end
        else Lwt_result.return j
      in
      append_from t (str, rest, j + n)

  let to_string rope =
    let* len = size rope in
    if len = 0
    then Lwt_result.return ""
    else begin
      let bytes = Bytes.create len in
      let+ q = blit_to_bytes rope 0 bytes 0 len in
      assert (q = len) ;
      Bytes.to_string bytes
    end

  let of_string str =
    let* t = create () in
    if str = "" then Lwt_result.return t else append t str

  let rec free t =
    let* height = get_height t in
    let+ () =
      if height = 0
      then Lwt_result.return ()
      else begin
        let* n = get_nb_children t in
        let rec go i =
          if i >= n
          then Lwt_result.return ()
          else if height = 1
          then begin
            let* child_ptr = get_child_ptr t i in
            Sector.free_ptr child_ptr ;
            go (i + 1)
          end
          else begin
            let* child = get_child t i in
            let* () = free child in
            go (i + 1)
          end
        in
        go 0
      end
    in
    Sector.free t

  let rec reachable_size t =
    let* height = get_height t in
    if height = 0
    then Lwt_result.return 1
    else
      let* nb = get_nb_children t in
      let rec go i acc =
        if i >= nb
        then Lwt_result.return acc
        else begin
          let* child = get_child t i in
          let* s = reachable_size child in
          go (i + 1) (acc + s)
        end
      in
      go 0 1
end

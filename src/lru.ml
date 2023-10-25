type 'a elt =
  | Nil
  | Removed
  | Detached of 'a
  | Elt of
      { value : 'a
      ; mutable prev : 'a elt
      ; mutable next : 'a elt
      }

type 'a t =
  { mutable first : 'a elt
  ; mutable last : 'a elt
  ; mutable length : int
  }

let make () = { first = Nil; last = Nil; length = 0 }

let push_front elt_elt t =
  match elt_elt with
  | Detached _ | Removed -> assert false
  | Nil -> assert false
  | Elt elt ->
    elt.prev <- Nil ;
    elt.next <- t.first ;
    begin
      match t.first with
      | Detached _ | Removed -> assert false
      | Nil ->
        assert (t.last = Nil) ;
        t.last <- elt_elt
      | Elt next ->
        assert (next.prev = Nil) ;
        next.prev <- elt_elt
    end ;
    t.first <- elt_elt

let peek_back t =
  match t.last with
  | Detached _ | Removed -> assert false
  | Nil ->
    assert (t.first = Nil) ;
    assert (t.length = 0) ;
    None
  | Elt e -> Some e.value

let pop_back t =
  match t.last with
  | Detached _ | Removed -> assert false
  | Nil ->
    assert (t.first = Nil) ;
    assert (t.length = 0) ;
    None
  | Elt elt as elt_elt ->
    assert (t.length > 0) ;
    t.length <- t.length - 1 ;
    t.last <- elt.prev ;
    begin
      match elt.prev with
      | Detached _ | Removed -> assert false
      | Nil ->
        assert (t.length = 0) ;
        assert (t.first == elt_elt) ;
        assert (t.last == Nil) ;
        t.first <- Nil
      | Elt prev -> prev.next <- Nil
    end ;
    assert (elt.next = Nil) ;
    elt.prev <- Removed ;
    elt.next <- Removed ;
    Some elt.value

let detach elt_elt t =
  match elt_elt with
  | Detached _ | Removed | Nil -> assert false
  | Elt { prev = Detached p; next = Detached n; value } ->
    assert (p == value) ;
    assert (n == value)
  | Elt ({ prev = Removed; next = Removed; _ } as elt) ->
    elt.next <- Detached elt.value ;
    elt.prev <- Detached elt.value
  | Elt elt ->
    begin
      match elt.prev with
      | Nil ->
        assert (t.first == elt_elt) ;
        t.first <- elt.next
      | Elt prev -> prev.next <- elt.next
      | Removed -> failwith "Lru.detach: Removed"
      | Detached _ -> failwith "Lru.detach: Detached"
    end ;
    begin
      match elt.next with
      | Nil ->
        assert (t.last == elt_elt) ;
        t.last <- elt.prev
      | Elt next -> next.prev <- elt.prev
      | Removed -> failwith "Lru.detach: Removed"
      | Detached _ -> failwith "Lru.detach: Detached"
    end ;
    assert (t.length > 0) ;
    t.length <- t.length - 1 ;
    elt.next <- Detached elt.value ;
    elt.prev <- Detached elt.value

let detach_remove elt_elt t =
  match elt_elt with
  | Detached _ | Removed | Nil -> assert false
  | Elt { prev = Detached p; next = Detached n; value } ->
    assert (p == value) ;
    assert (n == value) ;
    failwith "Lru.detached_remove: Detached"
  | Elt { prev = Removed; next = Removed; _ } -> ()
  | Elt elt ->
    begin
      match elt.prev with
      | Nil ->
        assert (t.first == elt_elt) ;
        t.first <- elt.next
      | Elt prev -> prev.next <- elt.next
      | Removed -> failwith "Lru.detach: Removed"
      | Detached _ -> failwith "Lru.detach: Detached"
    end ;
    begin
      match elt.next with
      | Nil ->
        assert (t.last == elt_elt) ;
        t.last <- elt.prev
      | Elt next -> next.prev <- elt.prev
      | Removed -> failwith "Lru.detach: Removed"
      | Detached _ -> failwith "Lru.detach: Detached"
    end ;
    assert (t.length > 0) ;
    t.length <- t.length - 1 ;
    elt.next <- Removed ;
    elt.prev <- Removed

let use elt_elt t =
  match elt_elt with
  | Detached _ -> ()
  | Removed -> assert false
  | Nil -> invalid_arg "Lru.use: Nil element"
  | Elt { prev = Removed; next = Removed; _ } ->
    (* do something to repair?.. *)
    t.length <- t.length + 1 ;
    push_front elt_elt t
  | Elt { prev = Nil; _ } -> assert (t.first == elt_elt)
  | Elt ({ prev = Elt prev as elt_prev; _ } as elt) ->
    prev.next <- elt.next ;
    begin
      match elt.next with
      | Detached _ | Removed -> assert false
      | Nil ->
        assert (t.last == elt_elt) ;
        t.last <- elt_prev
      | Elt next -> next.prev <- elt_prev
    end ;
    let prev_length = t.length in
    push_front elt_elt t ;
    assert (t.length = prev_length)
  | Elt { prev = Detached p; next = Detached n; value } ->
    assert (p == value) ;
    assert (n == value) ;
    failwith "Lru.use: unallocated"
  | Elt _ -> assert false

let make_elt value t =
  let elt = Elt { value; prev = Nil; next = t.first } in
  push_front elt t ;
  t.length <- t.length + 1 ;
  elt

let make_detached value = Detached value

let value = function
  | Removed -> invalid_arg "Lru.value: Removed"
  | Nil -> invalid_arg "Lru.value: Nil"
  | Elt { value; _ } -> value
  | Detached value -> value

let length t = t.length

let iter fn t =
  let rec go = function
    | Nil -> ()
    | Elt { value; next; _ } ->
      fn value ;
      go next
    | _ -> assert false
  in
  go t.first

let clear t =
  let rec go = function
    | Nil -> ()
    | Elt elt ->
      let next = elt.next in
      elt.prev <- Removed ;
      elt.next <- Removed ;
      go next
    | _ -> assert false
  in
  go t.first ;
  t.first <- Nil ;
  t.last <- Nil ;
  t.length <- 0

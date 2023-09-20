type outcome =
  [ `Success
  | `Cancelled
  | `Failure of string
  ]
[@@deriving irmin]

type status =
  [ `Success
  | `Cancelled
  | `Failure of string
  | `Running
  ]
[@@deriving irmin]

type t = outcome

let async fn : t =
  try
    fn () ;
    `Success
  with
  | err -> `Failure (Printexc.to_string err)

let cast = function
  | `Success -> `Success
  | `Cancelled -> `Cancelled
  | `Failure s -> `Failure s
  | `Running -> `Running
  | other -> other

let await : t -> [> outcome ] = function
  | `Success -> `Success
  | `Cancelled -> `Cancelled
  | `Failure s -> `Failure s

let status : t -> [> status ] = function
  | `Success -> `Success
  | `Cancelled -> `Cancelled
  | `Failure s -> `Failure s

let cancel (_ : t) = false

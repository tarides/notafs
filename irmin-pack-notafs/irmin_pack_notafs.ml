module Maker
    (Mclock : Mirage_clock.MCLOCK)
    (Pclock : Mirage_clock.PCLOCK)
    (Block : Mirage_block.S)
    (Config : Irmin_pack.Conf.S) =
struct
  module Io = Io.Make (Mclock) (Pclock) (Block)
  module Fs = Io.Fs
  include Irmin_pack_io.Maker_io (Io) (Io.Index_platform) (Async) (Config)
end

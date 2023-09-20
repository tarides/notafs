module Maker
    (Clock : Mirage_clock.MCLOCK)
    (Block : Notafs.DISK)
    (Config : Irmin_pack.Conf.S) =
struct
  module Io = Io.Make (Clock) (Block)
  module Fs = Io.Fs
  include Irmin_pack_io.Maker_io (Io) (Io.Index_platform) (Async) (Config)
end

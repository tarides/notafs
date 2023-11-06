module Block = struct
  include Block

  let discard _ _ = ()
  let flush _ = ()
end

module Disk = Notafs.KV (Notafs.Adler32) (Block)

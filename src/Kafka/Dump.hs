-----------------------------------------------------------------------------
-- |
-- Module providing various functions to dump information. These may be useful for 
-- debug/investigation but should probably not be used on production applications.
-----------------------------------------------------------------------------
module Kafka.Dump
( dumpKafkaConf
, dumpTopicConf
)
where

import Kafka.Internal.RdKafka
  ( CSizePtr
  , rdKafkaConfDumpFree
  , peekCText
  , rdKafkaConfDump
  , rdKafkaTopicConfDump
  )
import Kafka.Internal.Setup
  ( HasTopicConf(..)
  , HasKafkaConf(..)
  , getRdTopicConf
  , getRdKafkaConf
  )

import           Control.Monad          ((<=<))
import           Control.Monad.IO.Class (MonadIO(liftIO))
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Foreign                (Ptr, alloca, Storable(peek, peekElemOff))
import           Foreign.C.String       (CString)
import           Data.Text              (Text)

-- | Returns a map of the current topic configuration
dumpTopicConf :: (MonadIO m, HasTopicConf t) => t -> m (Map Text Text)
dumpTopicConf t = liftIO $ parseDump (rdKafkaTopicConfDump (getRdTopicConf t))

-- | Returns a map of the current kafka configuration
dumpKafkaConf :: (MonadIO m, HasKafkaConf k) => k -> m (Map Text Text)
dumpKafkaConf k = liftIO $ parseDump (rdKafkaConfDump (getRdKafkaConf k))

parseDump :: (CSizePtr -> IO (Ptr CString)) -> IO (Map Text Text)
parseDump cstr = alloca $ \sizeptr -> do
    strPtr <- cstr sizeptr
    size <- peek sizeptr

    keysAndValues <- mapM (peekCText <=< peekElemOff strPtr) [0..(fromIntegral size - 1)]

    let ret = Map.fromList $ listToTuple keysAndValues
    rdKafkaConfDumpFree strPtr size
    return ret

listToTuple :: [Text] -> [(Text, Text)]
listToTuple []       = []
listToTuple (k:v:ts) = (k, v) : listToTuple ts
listToTuple _        = error "list to tuple can only be called on even length lists"

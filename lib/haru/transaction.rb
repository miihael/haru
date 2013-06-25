require 'haru/ffihaildb'

module Haru

  #
  # A transaction in HailDB
  # http://www.innodb.com/doc/embedded_innodb-1.0/#id287624881
  #
  # A short example
  #
  #   require 'rubygems'
  #   require 'haru'
  #
  #   trx = Transaction.new
  #   trx.commit
  #
  #   trx = Transaction.new(Haru::SERIALIZABLE)
  #   trx.exclusive_schema_lock
  #   trx.commit
  #
  class Transaction

    # Creates a transaction with a specified isolation level and places
    # the transaction in the active state.
    #
    # == parameters
    #
    #   * trx_level   the isolation level to use for the transaction
    #
    def initialize(trx_level = READ_COMMITTED)
      @level = trx_level
      @trx_ptr = PureHailDB.ib_trx_begin(trx_level)
    end

    # 
    # return the statue the transaction is in
    def state()
      state = PureHailDB.ib_trx_state(@trx_ptr)
      if PureHailDB::TrxState[state] == PureHailDB::TrxState[:IB_TRX_NOT_STARTED]
        NOT_STARTED
      elsif PureHailDB::TrxState[state] == PureHailDB::TrxState[:IB_TRX_ACTIVE]
        ACTIVE
      elsif PureHailDB::TrxState[state] == PureHailDB::TrxState[:IB_TRX_COMMITTED_IN_MEMORY]
        COMMITTED_IN_MEMORY
      elsif PureHailDB::TrxState[state] == PureHailDB::TrxState[:IB_TRX_PREPARED]
        PREPARED
      else
      end
    end

    # Commits the transaction and releases the schema latches.
    def commit()
      check_return_code(PureHailDB.ib_trx_commit(@trx_ptr))
      release()
    end

    # Rolls back the transaction and releases the schema latches.
    def rollback()
      check_return_code(PureHailDB.ib_trx_rollback(@trx_ptr))
      release()
    end

    # Latches the HailDB data dictionary in exclusive mode
    def exclusive_schema_lock()
      check_return_code(PureHailDB.ib_schema_lock_exclusive(@trx_ptr))
      @schema_lock = true
    end

    def create_table(table)
      id_ptr = FFI::MemoryPointer.new(:int).write_int(0)
      check_return_code(PureHailDB.ib_table_create(@trx_ptr, table.schema_ptr.read_pointer(), id_ptr))
      # free the memory HailDB allocated
      PureHailDB.ib_table_schema_delete(table.schema_ptr.read_pointer())
    end

    def drop_table(table)
      check_return_code(PureHailDB.ib_table_drop(@trx_ptr, table.name))
    end

    def open_table(table)
      crs_ptr = FFI::MemoryPointer.new :pointer
      check_return_code(PureHailDB.ib_cursor_open_table(table.name, @trx_ptr, crs_ptr))
      cursor = Cursor.new(crs_ptr, table)
    end

    def release()
      return unless @trx_ptr
      PureHailDB.ib_schema_unlock(@trx_ptr) if @schema_lock
      @schema_lock = false
      #check_return_code(PureHailDB.ib_trx_release(@trx_ptr))
      @trx_ptr = nil
    end
  end

end

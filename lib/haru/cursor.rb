require 'haru/ffihaildb'

module Haru

  class Cursor

    INTENTION_EXCLUSIVE_LOCK = 1
    SHARED_LOCK = 2
    EXCLUSIVE_LOCK = 3

    attr_accessor :cursor_ptr

    def initialize(crs_ptr, input_table, parent_cursor=nil)
      @cursor_ptr = crs_ptr
      @table = input_table
      @parent_cursor = parent_cursor
    end

    def lock(lock_type = INTENTION_EXCLUSIVE_LOCK)
      check_return_code(PureHailDB.ib_cursor_lock(@cursor_ptr.read_pointer(), PureHailDB::LockMode[lock_type]))
    end

    def insert_row(row, ignore_duplicates=false)
      tuple_ptr = PureHailDB.ib_clust_read_tuple_create(@cursor_ptr.read_pointer())
      row.each_pair do |k,v|
        col = @table.column(k)
        raise "Column not found: #{k} in #{@table.name}" unless col
        col.insert_data(tuple_ptr, v)
      end
      ret = PureHailDB.ib_cursor_insert_row(@cursor_ptr.read_pointer, tuple_ptr)
      if ignore_duplicates and PureHailDB::DbError[ret] == PureHailDB::DbError[:DB_DUPLICATE_KEY]
        ret = :DB_SUCCESS
      end
      PureHailDB.ib_tuple_delete(tuple_ptr)
      return ret
    end

    def read_row(tuple_ptr=nil)
      tptr = tuple_ptr
      if not tptr
        tptr = PureHailDB.ib_clust_read_tuple_create(@cursor_ptr.read_pointer())
      end
      check_return_code(PureHailDB.ib_cursor_read_row(@cursor_ptr.read_pointer(), tptr))
      cols = @table.columns
      row = {}
      cols.each_pair do |k,v|
        row[k] = v.get_data(tptr)
      end
      if tuple_ptr
        PureHailDB.ib_tuple_clear(tptr)
      else
        PureHailDB.ib_tuple_delete(tptr)
      end
      return row
    end

    def prev_row()
      check_return_code(PureHailDB.ib_cursor_prev(@cursor_ptr.read_pointer()))
    end

    def next_row()
      check_return_code( PureHailDB.ib_cursor_next(@cursor_ptr.read_pointer()) )
    end

    def first_row()
      check_return_code(PureHailDB.ib_cursor_first(@cursor_ptr.read_pointer()))
    end

    def last_row()
      check_return_code(PureHailDB.ib_cursor_last(@cursor_ptr.read_pointer()))
    end

    def iterate_read(limit=nil)
      cnt = 0
      err = first_row()
      tuple_ptr = PureHailDB.ib_clust_read_tuple_create(@cursor_ptr.read_pointer())
      while err == :DB_SUCCESS do
        r = read_row(tuple_ptr)
        yield r
        err = next_row()
        cnt+=1
        break if limit and cnt>=limit
      end
      PureHailDB.ib_tuple_delete(tuple_ptr) if tuple_ptr
      return cnt
    end

    def secondary_cursor(idx_name)
      idx_cur_ptr = FFI::MemoryPointer :pointer
      check_return_code(PureHailDB.ib_cursor_open_index_using_name(
                  @cursor_ptr.read_pointer(), idx_name, idx_cur_ptr))
      return Cursor.new(idx_cur_ptr, @table, self)
    end

    def moveto(cols_data, match_mode, search_mode) #cols_data is array of [ column_obj, search_value ]
      tuple_ptr = nil
      if @parent
        tuple_ptr = PureHailDB.ib_sec_search_tuple_create(@cursor_ptr.read_pointer())
      else
        tuple_ptr = PureHailDB.ib_clust_search_tuple_create(@cursor_ptr.read_pointer())
      end
      i = 0
      cols_data.each do |cd|
         cd[0].insert_data(tuple_ptr, cd[1], i)
         i += 1
      end
      PureHailDB.ib_cursor_set_match_mode(@cursor_ptr.read_pointer(), match_mode)
      PureHailDB.ib_cursor_set_cluster_access(@cursor_ptr.read_pointer()) if @parent

      res_ptr = FFI::MemoryPointer.new :uint32
      err = PureHailDB.ib_cursor_moveto(@cursor_ptr.read_pointer(),
                                        tuple_ptr, search_mode, res_ptr)
      p err, res_ptr.read_int()
      return [ err, tuple_ptr ]
    end

    def iterate_search(idx_name, cols_data, match_mode=:IB_CLOSEST_MATCH, search_mode=:IB_CUR_GE, limit=nil)
      cnt = 0
      if not @parent
        sec_cursor = secondary_cursor(idx_name)
        sec_cursor.iterate_search(idx_name, cols_data, match_mode, search_mode, limit) do |r|
          yield r
        end
        sec_cursor.close()
      else
        err, search_tptr = moveto(cols_data, match_mode, search_mode)
        tuple_ptr = PureHailDB.ib_clust_read_tuple_create(@cursor_ptr.read_pointer())
        while err == :DB_SUCCESS do
          r = read_row(tuple_ptr)
          p r
          if (cols_data.all? { |cd| cd[1] == r[cd[0].name] })
            cnt += 1
            yield r
          end
          err = next_row()
          break if limit and cnt>=limit
        end
        PureHailDB.ib_tuple_delete(tuple_ptr) if tuple_ptr
        PureHailDB.ib_tuple_delete(search_tptr) if search_tptr
        return [ err, cnt ]
      end
    end


    def reset()
      check_return_code(PureHailDB.ib_cursor_reset(@cursor_ptr.read_pointer()))
    end

    def close(tuple_ptr=nil)
      check_return_code(PureHailDB.ib_cursor_close(@cursor_ptr.read_pointer()))
    end

  end

end

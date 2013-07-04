require 'haru/ffihaildb'

module Haru

  class Column

    attr_accessor :num
    attr_accessor :name
    attr_accessor :type
    attr_accessor :attrs
    attr_accessor :size

    def initialize(col_name, col_type, col_attrs, col_size, col_num)
      @name = col_name
      @type = col_type
      @attrs = col_attrs
      @size = col_size
      @num = col_num
    end

    def insert_data(tuple_ptr, data, num=nil)
      num ||= @num
      case @type
      when INT
        data = data.to_i if data.is_a?(String)
        if @size==2 then
          check_return_code(PureHailDB.ib_tuple_write_u16(tuple_ptr, num, data))
        end
        if @size==4 then
          check_return_code(PureHailDB.ib_tuple_write_u32(tuple_ptr, num, data))
        end
        if @size==8 then
          check_return_code(PureHailDB.ib_tuple_write_u64(tuple_ptr, num, data))
        end

      when FLOAT
        data = data.to_f if data.is_a?(String)
        check_return_code(PureHailDB.ib_tuple_write_float(tuple_ptr,
                                                          num,
                                                          data))
      when DOUBLE
        data = data.to_f if data.is_a?(String)
        check_return_code(PureHailDB.ib_tuple_write_double(tuple_ptr,
                                                           num,
                                                           data))
      when CHAR
        p = FFI::MemoryPointer.from_string(data)
        check_return_code(PureHailDB.ib_col_set_value(tuple_ptr,
                                                      num,
                                                      p,
                                                      @size))
      when BLOB
      WHEN DECIMAL
      when VARCHAR
        vp = FFI::MemoryPointer.from_string(data)
        check_return_code(PureHailDB.ib_col_set_value(tuple_ptr,
                                                      num,
                                                      vp,
                                                      data.size))
      end
    end

    def get_data(tuple_ptr)
      case @type
      when INT
        if @size==2 then
          res_ptr = FFI::MemoryPointer.new :uint32
          check_return_code(PureHailDB.ib_tuple_read_u16(tuple_ptr, @num, res_ptr))
          return res_ptr.read_int()
        end
        if @size==4 then
          res_ptr = FFI::MemoryPointer.new :uint32
          check_return_code(PureHailDB.ib_tuple_read_u32(tuple_ptr, @num, res_ptr))
          return res_ptr.read_int()
        end
        if @size==8 then
          res_ptr = FFI::MemoryPointer.new :uint64
          check_return_code(PureHailDB.ib_tuple_read_u64(tuple_ptr, @num, res_ptr))
          return res_ptr.read_int()
        end
      when FLOAT
        res_ptr = FFI::MemoryPointer.new :float
        check_return_code(PureHailDB.ib_tuple_read_float(tuple_ptr, @num, res_ptr))
        res_ptr.read_float()
      when DOUBLE
        res_ptr = FFI::MemoryPointer.new :double
        check_return_code(PureHailDB.ib_tuple_read_double(tuple_ptr, @num, res_ptr))
        res_ptr.read_double()
      when CHAR
      when BLOB
      when DECIMAL
      when VARCHAR
        vlen = PureHailDB.ib_col_get_len(tuple_ptr, @num)
        return nil if not vlen or vlen == 0
        res_ptr = PureHailDB.ib_col_get_value(tuple_ptr, @num)
        res_ptr.get_string(0, vlen)
      end
    end

  end

  class Table

    attr_accessor :name
    attr_accessor :schema_ptr
    attr_accessor :columns
    attr_accessor :indexes

    def self.load(tbl_obj)
      t = Table.new(*tbl_obj.name.split("/"))
      t.create(tbl_obj.columns.values.sort_by { |c| c.num }.map { |c| [c.name, c.type, c.attrs, c.size ] })
      if not t.exists()
        tbl_obj.indexes.each_item { |iname, tcols|
            case tcols[0]
              when SECONDARY_INDEX
                t.add_index(iname, *tcols[1..-1])
              when CLUSTERED_INDEX
                t.add_clustered_index(iname, *tcols[1..-1])
            end
        }
      end
      return t
    end


    def initialize(db_name, table_name)
      @name = db_name + "/" + table_name
      @schema_ptr = FFI::MemoryPointer.new :pointer
      @columns = {}
      @indexes = {}
      @page_size = 0
    end

    def exists()
      id_ptr = FFI::MemoryPointer.new :pointer
      return false if PureHailDB.ib_table_get_id(@name, id_ptr) != :DB_SUCCESS
      @exists = true
      return true
    end

    def create(cols=nil)
      check_return_code(PureHailDB.ib_table_schema_create(@name, @schema_ptr,
                                                            PureHailDB::TableFormat[:IB_TBL_COMPACT],
                                                            @page_size)) unless exists()
      add_columns(cols) if cols
    end

    def add_column(col_name, col_type, col_attrs, col_size)
      c = Column.new(col_name, col_type, col_attrs, col_size, @columns.size)
      @columns[col_name] = c
      return if @exists
      check_return_code(PureHailDB.ib_table_schema_add_col(@schema_ptr.read_pointer(),
                                                           col_name,
                                                           PureHailDB::ColumnType[col_type],
                                                           col_attrs,
                                                           0,
                                                           col_size))
    end

    def add_columns(cols)
       cols.each do |r|
         add_column(*r)
       end
    end

    def add_integer_column(col_name)
      c = Column.new(col_name, INT, UNSIGNED, 4, @columns.size)
      @columns[col_name] = c
      return if @exists
      check_return_code(PureHailDB.ib_table_schema_add_col(@schema_ptr.read_pointer(),
                                                           col_name,
                                                           PureHailDB::ColumnType[INT],
                                                           PureHailDB::ColumnAttr[UNSIGNED],
                                                           0,
                                                           4))
    end

    def add_bigint_column(col_name)
      c = Column.new(col_name, INT, UNSIGNED, 8, @columns.size)
      @columns[col_name] = c
      return if @exists
      check_return_code(PureHailDB.ib_table_schema_add_col(@schema_ptr.read_pointer(),
                                                           col_name,
                                                           PureHailDB::ColumnType[INT],
                                                           PureHailDB::ColumnAttr[UNSIGNED],
                                                           0,
                                                           8))
    end


    def add_string_column(col_name, col_size)
      c = Column.new(col_name, VARCHAR, NONE, col_size, @columns.size)
      @columns[col_name] = c
      return if @exists
      check_return_code(PureHailDB.ib_table_schema_add_col(@schema_ptr.read_pointer(),
                                                           col_name,
                                                           PureHailDB::ColumnType[VARCHAR],
                                                           PureHailDB::ColumnAttr[NONE],
                                                           0,
                                                           col_size))
    end

    def add_fixed_size_string_column(col_name, col_size)
      c = Column.new(col_name, CHAR, NONE, col_size, @columns.size)
      @columns[col_name] = c
      return if @exists
      check_return_code(PureHailDB.ib_table_schema_add_col(@schema_ptr.read_pointer(),
                                                           col_name,
                                                           PureHailDB::ColumnType[CHAR],
                                                           PureHailDB::ColumnAttr[NONE],
                                                           0,
                                                           col_size))
    end

    def create_index(idx_name)
      idx_ptr = FFI::MemoryPointer.new :pointer
      check_return_code(PureHailDB.ib_table_schema_add_index(@schema_ptr.read_pointer(), idx_name, idx_ptr))
      return idx_ptr
    end

    def add_index(idx_name, col_name, *other_col_names)
      idx_ptr = create_index(idx_name)
      check_return_code(PureHailDB.ib_index_schema_add_col(idx_ptr.read_pointer(), col_name, 0))
      other_col_names.each do |col|
        check_return_code(PureHailDB.ib_index_schema_add_col(idx_ptr.read_pointer(), col, 0))
      end
      @indexes[idx_name] = [SECONDARY_INDEX, col_name ] + other_col_names
      return idx_ptr
    end

    def add_clustered_index(idx_name, col_name, *other_col_names)
      idx_ptr = add_index(idx_name, col_name, *other_col_names)
      @indexes[idx_name][0] = CLUSTERED_INDEX
      check_return_code(PureHailDB.ib_index_schema_set_clustered(idx_ptr.read_pointer()))
    end

    def column(col_name)
      @columns[col_name]
    end

    def truncate()
      id_ptr = FFI::MemoryPointer.new :pointer
      check_return_code(PureHailDB.ib_table_truncate(@name, id_ptr))
    end
  end

end

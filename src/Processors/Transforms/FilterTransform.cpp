#include <Processors/Transforms/FilterTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#define THROW_WXS_NOT_OK(status) \
    do \
    { \
        if (::arrow::Status _s = (status); !_s.ok()) \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

namespace DB
{

static void replaceFilterToConstant(Block & block, const String & filter_column_name)
{
    ConstantFilterDescription constant_filter_description;

    auto filter_column = block.getPositionByName(filter_column_name);
    auto & column_elem = block.safeGetByPosition(filter_column);

    /// Isn't the filter already constant?
    if (column_elem.column)
        constant_filter_description = ConstantFilterDescription(*column_elem.column);

    if (!constant_filter_description.always_false && !constant_filter_description.always_true)
    {
        /// Replace the filter column to a constant with value 1.
        FilterDescription filter_description_check(*column_elem.column);
        column_elem.column = column_elem.type->createColumnConst(block.rows(), 1u);
    }
}

static void ChunkToParquet(Chunk & chunk, WriteBuffer & buffer, const Block & header, Poco::Logger * log)
{
    UNUSED(log);
    const size_t columns_num = chunk.getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;
    CHColumnToArrowColumn::chChunkToArrowTable(arrow_table, header, chunk, columns_num, "Parquet");
    std::unique_ptr<parquet::arrow::FileWriter> file_writer;
    {
        auto sink = std::make_shared<ArrowBufferedOutputStream>(buffer);
        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::SNAPPY);
        auto props = builder.build();
        auto status = parquet::arrow::FileWriter::Open(*arrow_table->schema(), arrow::default_memory_pool(), sink, props, &file_writer);
        if (!status.ok())
            throw Exception{"Error while opening a table: " + status.ToString(), 1};
    }
    auto status = file_writer->WriteTable(*arrow_table, 1000000);
    if (!status.ok())
        throw Exception{"Error while writing a table: " + status.ToString(), 1};
}

static void ParquetToChunk(Chunk & chunk, ReadBuffer & buffer, const Block & header, Poco::Logger * log)
{
    UNUSED(log);
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    THROW_WXS_NOT_OK(parquet::arrow::OpenFile(asArrowFile(buffer), arrow::default_memory_pool(), &file_reader));
    std::shared_ptr<arrow::Schema> schema;
    THROW_WXS_NOT_OK(file_reader->GetSchema(&schema));
    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->ReadTable(&table);
    if (!read_status.ok())
        throw Exception{"Error while reading Parquet data: " + read_status.ToString(), 1};
    try
    {
        ArrowColumnToCHColumn::arrowTableToCHChunk(chunk, table, header, "Parquet");
    }
    catch (...)
    {
        LOG_INFO(log, "ArrowColumnToCHColumn::arrowTableToCHChunk exception");
    }
}

Block FilterTransform::transformHeader(
    Block header, const ExpressionActionsPtr & expression, const String & filter_column_name, bool remove_filter_column)
{
    expression->execute(header);

    if (remove_filter_column)
        header.erase(filter_column_name);
    else
        replaceFilterToConstant(header, filter_column_name);

    return header;
}

FilterTransform::FilterTransform(
    const Block & header_, ExpressionActionsPtr expression_, String filter_column_name_, bool remove_filter_column_, bool on_totals_)
    : ISimpleTransform(header_, transformHeader(header_, expression_, filter_column_name_, remove_filter_column_), true)
    , expression(std::move(expression_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
    , times(0)
    , step(Null)
    , log(&Poco::Logger::get("FilterTransform"))
{
    transformed_header = getInputPort().getHeader();
    expression->execute(transformed_header);
    filter_column_position = transformed_header.getPositionByName(filter_column_name);

    auto & column = transformed_header.getByPosition(filter_column_position).column;
    if (column)
        constant_filter_description = ConstantFilterDescription(*column);
    std::string host = "127.0.0.1";
    int port = 6379;
    redis_client.connect(host, port);
}

IProcessor::Status FilterTransform::prepare()
{
    if (!on_totals
        && (constant_filter_description.always_false
            /// Optimization for `WHERE column in (empty set)`.
            /// The result will not change after set was created, so we can skip this check.
            /// It is implemented in prepare() stop pipeline before reading from input port.
            || (!are_prepared_sets_initialized && expression->checkColumnIsAlwaysFalse(filter_column_name))))
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    auto func = [&]() {
        if (output.isFinished())
        {
            input.close();
            return Status::Finished;
        }

        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        /// Output if has data.
        if (has_output)
        {
            output.pushData(std::move(output_data));
            has_output = false;

            if (!no_more_data_needed)
                return Status::PortFull;
        }

        /// Stop if don't need more data.
        if (no_more_data_needed)
        {
            input.close();
            output.finish();
            return Status::Finished;
        }

        /// Check can input.
        if (!has_input)
        {
            if (input.isFinished())
            {
                output.finish();
                return Status::Finished;
            }

            input.setNeeded();

            updateStep();
            if (step == LoadInMemory)
            {
                std::string key = cache_filter_column_name + "_" + std::to_string(wangxinshuo_index) + "_" + std::to_string(times++);
                auto reply = redis_client.get(const_cast<char *>(key.c_str()), key.length());
                if (reply->type == 4)
                {
                    input.close();
                    output.finish();
                    return Status::Finished;
                }
                else
                {
                    ReadBuffer rb(reply->str, reply->len, 0);
                    Chunk chunk;
                    ParquetToChunk(chunk, rb, output.getHeader(), log);
                    input_data = Port::Data{.chunk = std::move(chunk), .exception = nullptr};
                    has_input = true;
                }
            }
            else
            {
                if (!input.hasData())
                    return Status::NeedData;

                input_data = input.pullData(set_input_not_needed_after_read);
                has_input = true;
            }


            if (input_data.exception)
                /// No more data needed. Exception will be thrown (or swallowed) later.
                input.setNotNeeded();
        }

        /// Now transform.
        return Status::Ready;
    };
    auto status = func();

    /// Until prepared sets are initialized, output port will be unneeded, and prepare will return PortFull.
    if (status != IProcessor::Status::PortFull)
        are_prepared_sets_initialized = true;

    return status;
}


void FilterTransform::removeFilterIfNeed(Chunk & chunk) const
{
    if (chunk && remove_filter_column)
        chunk.erase(filter_column_position);
}

void FilterTransform::transform(Chunk & chunk)
{
    size_t num_rows_before_filtration;
    auto columns = chunk.detachColumns();

    {
        Block block = getInputPort().getHeader().cloneWithColumns(columns);
        columns.clear();

        expression->execute(block);

        num_rows_before_filtration = block.rows();
        columns = block.getColumns();
    }

    if (constant_filter_description.always_true || on_totals)
    {
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    size_t num_columns = columns.size();
    ColumnPtr filter_column = columns[filter_column_position];

    /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
        *  and now - are calculated. That is, not all cases are covered by the code above.
        * This happens if the function returns a constant for a non-constant argument.
        * For example, `ignore` function.
        */
    constant_filter_description = ConstantFilterDescription(*filter_column);

    if (constant_filter_description.always_false)
        return; /// Will finish at next prepare call

    if (constant_filter_description.always_true)
    {
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    FilterDescription filter_and_holder(*filter_column);

    /** Let's find out how many rows will be in result.
      * To do this, we filter out the first non-constant column
      *  or calculate number of set bytes in the filter.
      */
    size_t first_non_constant_column = num_columns;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != filter_column_position && !isColumnConst(*columns[i]))
        {
            first_non_constant_column = i;
            break;
        }
    }

    size_t num_filtered_rows = 0;
    if (first_non_constant_column != num_columns)
    {
        columns[first_non_constant_column] = columns[first_non_constant_column]->filter(*filter_and_holder.data, -1);
        num_filtered_rows = columns[first_non_constant_column]->size();
    }
    else
        num_filtered_rows = countBytesInFilter(*filter_and_holder.data);

    /// If the current block is completely filtered out, let's move on to the next one.
    if (num_filtered_rows == 0)
        /// SimpleTransform will skip it.
        return;

    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        if (!remove_filter_column)
        {
            /// Replace the column with the filter by a constant.
            auto & type = transformed_header.getByPosition(filter_column_position).type;
            columns[filter_column_position] = type->createColumnConst(num_filtered_rows, 1u);
        }

        /// No need to touch the rest of the columns.
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & current_type = transformed_header.safeGetByPosition(i).type;
        auto & current_column = columns[i];

        if (i == filter_column_position)
        {
            /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
            /// NOTE User could pass column with something different than 0 and 1 for filter.
            /// Example:
            ///  SELECT materialize(100) AS x WHERE x
            /// will work incorrectly.
            current_column = current_type->createColumnConst(num_filtered_rows, 1u);
            continue;
        }

        if (i == first_non_constant_column)
            continue;

        if (isColumnConst(*current_column))
            current_column = current_column->cut(0, num_filtered_rows);
        else
            current_column = current_column->filter(*filter_and_holder.data, num_filtered_rows);
    }

    chunk.setColumns(std::move(columns), num_filtered_rows);
    removeFilterIfNeed(chunk);
}

void FilterTransform::transform(Chunk & input_chunk, Chunk & output_chunk)
{
    updateStep();
    if (step == StoreToRedis)
    {
        transform(input_chunk);
        output_chunk.swap(input_chunk);
        std::string key = filter_column_name + "_" + std::to_string(wangxinshuo_index) + "_" + std::to_string(times++);
        const size_t buffer_size = 1 * 1024 * 1024;
        char * buffer = new char[buffer_size];
        WriteBuffer wb(buffer, buffer_size);
        ChunkToParquet(output_chunk, wb, output.getHeader(), log);
        redis_client.set(key.c_str(), key.length(), buffer, wb.offset());
        delete[] buffer;
    }
    else
    {
        transform(input_chunk);
        output_chunk.swap(input_chunk);
    }
}

static bool canParse(std::string & s)
{
    std::string disallow[] = {"notEmpty", "extractAll", "arrayJoin", "is_aggregate"};
    for (auto x : disallow)
    {
        if (s.find(x) != std::string::npos)
        {
            return false;
        }
    }
    return true;
}

void FilterTransform::updateStep()
{
    if (step == Null)
    {
        const std::string prefix = "sub_filter_" + std::to_string(wangxinshuo_index) + "_";
        std::string regex = prefix + "*";
        auto reply = redis_client.keys(const_cast<char *>(regex.c_str()), regex.length());

        LOG_INFO(log, canParse(filter_column_name) ? "dosen't find disallow word" : "find disallow word");

        bool exist = false;
        if (canParse(filter_column_name))
        {
            FilterTree tree(filter_column_name);
            for (size_t i = 0; i < reply->elements; ++i)
            {
                std::string key(std::string(reply->element[i]->str), prefix.length());
                LOG_INFO(log, "reply {} element is {}, match_tree key is {}", i, std::string(reply->element[i]->str), key);
                if (!canParse(key))
                    continue;
                FilterTree match_tree(key);
                if (match_tree.Contain(tree))
                {
                    LOG_INFO(log, "find match cache {}", key);
                    exist = true;
                    cache_filter_column_name = key;
                    break;
                }
            }
        }


        if (exist)
        {
            LOG_INFO(log, "exist in cache, load into memoty.");
            step = LoadInMemory;
        }
        else
        {
            LOG_INFO(log, "not exist in cache");
            step = StoreToRedis;
            std::string key = "sub_filter_" + std::to_string(wangxinshuo_index) + "_" + filter_column_name;
            std::string value = key;
            redis_client.set(key.c_str(), key.length(), value.c_str(), value.length());
        }
    }
}

}

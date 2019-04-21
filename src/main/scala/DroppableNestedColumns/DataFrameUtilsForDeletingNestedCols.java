package DroppableNestedColumns;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
// You have to statically IMPORT these in the code.
import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;





/**
 * @author vdokku
 */
public class DataFrameUtilsForDeletingNestedCols {

    public static Dataset<Row> dropNestedColumn(Dataset<Row> dataFrame, String columnName) {
        final DataFrameFolder dataFrameFolder = new DataFrameFolder(dataFrame);
        Arrays.stream(dataFrame.schema().fields())
                .flatMap(f -> {
                    if (columnName.startsWith(f.name() + ".")) {
                        final Optional<Column> column = dropSubColumn(col(f.name()), f.dataType(), f.name(), columnName);
                        if (column.isPresent()) {
                            return Stream.of(new Tuple2<>(f.name(), column));
                        } else {
                            return Stream.empty();
                        }
                    } else {
                        return Stream.empty();
                    }
                }).forEachOrdered(colTuple -> dataFrameFolder.accept(colTuple));

        return dataFrameFolder.getDF();
    }

    private static Optional<Column> dropSubColumn(Column col, DataType colType, String fullColumnName, String dropColumnName) {
        Optional<Column> column = Optional.empty();
        if (!fullColumnName.equals(dropColumnName)) {
            if (colType instanceof StructType) {
                if (dropColumnName.startsWith(fullColumnName + ".")) {
                    column = Optional.of(struct(getColumns(col, (StructType) colType, fullColumnName, dropColumnName)));
                }
            } else {
                column = Optional.of(col);
            }
        }

        return column;
    }

    private static Column[] getColumns(Column col, StructType colType, String fullColumnName, String dropColumnName) {
        return Arrays.stream(colType.fields())
                .flatMap(f -> {
                            final Optional<Column> column = dropSubColumn(col.getField(f.name()), f.dataType(),
                                    fullColumnName + "." + f.name(), dropColumnName);
                            if (column.isPresent()) {
                                return Stream.of(column.get().alias(f.name()));
                            } else {
                                return Stream.empty();
                            }
                        }
                ).toArray(Column[]::new);

    }

    private static class DataFrameFolder implements Consumer<Tuple2<String, Optional<Column>>> {
        private Dataset<Row> df;

        public DataFrameFolder(Dataset<Row> df) {
            this.df = df;
        }

        public Dataset<Row> getDF() {
            return df;
        }

        @Override
        public void accept(Tuple2<String, Optional<Column>> colTuple) {
            if (!colTuple._2().isPresent()) {
                df = df.drop(colTuple._1());
            } else {
                df = df.withColumn(colTuple._1(), colTuple._2().get());
            }
        }
    }

}

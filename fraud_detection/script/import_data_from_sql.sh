jar_path=/data/dongjiahao/svnProject/basedata/trunk/target
package=hadoop_test-1.0-jar-with-dependencies.jar

tmp_dir=/mroutput/dongjiahao/new_basedata
import_dir=$tmp_dir/dsp3_import
import_data_class_name=com.mr.merge_data.ImportDataFromSQL
# hadoop dfs -rmr -skipTrash $import_dir
hadoop jar $jar_path/$package $import_data_class_name $import_dir
# hadoop dfs -chmod 777 $tmp_dir
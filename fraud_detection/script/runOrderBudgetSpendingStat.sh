jar_path=/data/rongyifei/svn/basedata/trunk/target
package=hadoop_test-1.0-jar-with-dependencies.jar

tmp_dir=/share_data/tmp
import_dir=$tmp_dir/dsp3_import

hadoop jar $jar_path/$package com.mr.newbasedata.FetchCpcOrders $import_dir

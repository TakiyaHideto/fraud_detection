cd log/

svn up

src_package=com.yoyi.log.types
des_package=com.mr.protobuffer

bid_log=original_bid.proto
win_log=original_win.proto
imp_log=original_show.proto
clk_log=original_click.proto
reach_log=original_reach.proto

sed 's/\(java_package="\)'"$src_package"'"/\1'"$des_package"'"/g' $bid_log > tmp
mv tmp $bid_log
sed 's/\(java_package="\)'"$src_package"'"/\1'"$des_package"'"/g' $win_log > tmp
mv tmp $win_log
sed 's/\(java_package="\)'"$src_package"'"/\1'"$des_package"'"/g' $imp_log > tmp
mv tmp $imp_log
sed 's/\(java_package="\)'"$src_package"'"/\1'"$des_package"'"/g' $clk_log > tmp
mv tmp $clk_log
sed 's/\(java_package="\)'"$src_package"'"/\1'"$des_package"'"/g' $reach_log > tmp
mv tmp $reach_log
#protoc *.proto --java_out=../../../../


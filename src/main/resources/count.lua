function countMapItems(stream, mapBinName)
	local function aggregate_stats(count,rec)
		local aMap = rec[mapBinName]
		if aMap == nil then
			return count
		end
		return count + map.size(aMap)
	end
	
	local function reduce_stats(a,b)
		return a+b
	end
	
	local function root_block_filter(rec) 
		local val = rec.root_blk;
		if val == nil or val < 2 then
			return true;
		else
			return false
		end
	end

	return stream : filter(root_block_filter) : aggregate(0,aggregate_stats) : reduce(reduce_stats)
end

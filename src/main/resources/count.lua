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

	return stream : aggregate(0,aggregate_stats) : reduce(reduce_stats)
end

-- Bounded, atomic inspection + deletion. Never infer ACK from age or length.
-- Only the queue payload is disposable; reservations/states remain untouched.
local ty=redis.call('TYPE',KEYS[1]).ok
if ty=='none' then return {'0-0',0,0,0} end
local groups=redis.call('XINFO','GROUPS',KEYS[1])
if #groups==0 or #groups>8 then return {'0-0',0,0,1} end
local function le(a,b)
 local am,as=string.match(a,'^(%d+)%-(%d+)$')
 local bm,bs=string.match(b,'^(%d+)%-(%d+)$')
 if #am~=#bm then return #am<#bm end
 if am~=bm then return am<bm end
 if #as~=#bs then return #as<#bs end
 return as<=bs
end
local bounds={}; local owned=false
for _,g in ipairs(groups) do
 local name; local last
 for i=1,#g,2 do
  if g[i]=='name' then name=g[i+1] end
  if g[i]=='last-delivered-id' then last=g[i+1] end
 end
 if name=='relay' then owned=true end
 local pending=redis.call('XPENDING',KEYS[1],name)
 bounds[#bounds+1]={name,last,pending[2]}
end
if not owned then return {'0-0',0,0,1} end
local count=128
local rows=redis.call('XRANGE',KEYS[1],'('..ARGV[1],'+','COUNT',count)
local deleted=0; local cursor='0-0'
for _,row in ipairs(rows) do
 local mid=row[1]; local safe=true
 for _,b in ipairs(bounds) do
  if not le(mid,b[2]) then safe=false; break end
  -- Before the earliest PEL item (or with no PEL), delivery implies ACK.
  if b[3] and le(b[3],mid) and #redis.call('XPENDING',KEYS[1],b[1],mid,mid,1)>0 then
   safe=false; break
  end
 end
 if safe then deleted=deleted+redis.call('XDEL',KEYS[1],mid) end
 cursor=mid
end
if #rows<count then cursor='0-0' end
return {cursor,deleted,#rows,0}

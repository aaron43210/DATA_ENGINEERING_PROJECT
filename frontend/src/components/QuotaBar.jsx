export default function QuotaBar({ callsToday, quotaDay }) {
  const pct = quotaDay > 0 ? Math.min(100, (callsToday / quotaDay) * 100) : 0
  const exhausted = callsToday >= quotaDay

  let barColor = 'bg-green-500'
  if (pct > 80) barColor = 'bg-red-500'
  else if (pct >= 60) barColor = 'bg-amber-400'

  return (
    <div className="w-full">
      <div className="flex justify-between text-xs text-gray-600 mb-1">
        <span>{callsToday} / {quotaDay} calls</span>
        {exhausted
          ? <span className="text-red-600 font-medium">Exhausted — resets tomorrow</span>
          : pct > 80
            ? <span className="text-red-600">{pct.toFixed(0)}%</span>
            : pct >= 60
              ? <span className="text-amber-600">{pct.toFixed(0)}%</span>
              : <span>{pct.toFixed(0)}%</span>
        }
      </div>
      <div className="h-1.5 w-full bg-gray-200 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${barColor}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}

import React from 'react';
import { 
  Receipt, 
  IndianRupee, 
  LayoutList, 
  Banknote, 
  Store, 
  Info,
  RefreshCw
} from 'lucide-react';
import { useSegmentStore } from '../../store/segmentStore';
import { clsx } from 'clsx';

const AudienceSummaryPanel: React.FC = () => {
  const { summaryMetrics, isFetchingSummary, audienceCount } = useSegmentStore();

  if (!audienceCount && !isFetchingSummary) return null;

  const formatNumber = (val: number | undefined) => {
    if (val === undefined) return '—';
    return new Intl.NumberFormat('en-IN').format(Math.round(val));
  };

  const formatCurrency = (val: number | undefined) => {
    if (val === undefined) return '—';
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 2,
    }).format(val);
  };

  const metrics = [
    { 
      label: 'Total Bills', 
      value: formatNumber(summaryMetrics?.total_bills), 
      icon: Receipt,
      color: 'bg-indigo-50 text-indigo-600'
    },
    { 
      label: 'Total Spend', 
      value: formatCurrency(summaryMetrics?.total_spend), 
      icon: IndianRupee,
      color: 'bg-blue-50 text-blue-600'
    },
    { 
      label: 'Average Spend', 
      value: formatCurrency(summaryMetrics?.avg_spend), 
      icon: IndianRupee,
      color: 'bg-violet-50 text-violet-600'
    },
    { 
      label: 'Spend per Bill', 
      value: formatCurrency(summaryMetrics?.spend_per_bill), 
      icon: Banknote,
      color: 'bg-indigo-50 text-indigo-600'
    },
    { 
      label: 'Spend per Visit', 
      value: formatCurrency(summaryMetrics?.spend_per_visit), 
      icon: Store,
      color: 'bg-blue-50 text-blue-600'
    },
  ];

  return (
    <div className="bg-white rounded-xl border border-slate-200 overflow-hidden mt-4">
      <div className="p-4 border-b border-slate-100 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <h3 className="font-semibold text-slate-800">Summary Statistics</h3>
          <Info className="w-4 h-4 text-slate-400 cursor-help" />
        </div>
      </div>

      <div className="p-4 space-y-4">
        {isFetchingSummary ? (
          <div className="space-y-4 animate-pulse">
            {[...Array(6)].map((_, i) => (
              <div key={i} className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg bg-slate-100" />
                  <div className="w-24 h-4 bg-slate-100 rounded" />
                </div>
                <div className="w-16 h-4 bg-slate-100 rounded" />
              </div>
            ))}
          </div>
        ) : (
          <div className="space-y-3">
            {metrics.map((m, idx) => (
              <div key={idx} className="flex items-center justify-between group">
                <div className="flex items-center gap-3">
                  <div className={clsx("p-1.5 rounded-lg", m.color)}>
                    <m.icon className="w-4 h-4" />
                  </div>
                  <span className="text-sm font-medium text-slate-600">{m.label}</span>
                </div>
                <span className="text-sm font-bold text-slate-900">{m.value}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="bg-slate-50 px-4 py-2 flex items-center justify-between border-t border-slate-100">
        <span className="text-[10px] text-slate-400 italic">
          Metrics calculated on estimated audience
        </span>
        <div className="flex items-center gap-1 text-[10px] text-slate-400">
          <RefreshCw className={clsx("w-3 h-3", isFetchingSummary && "animate-spin")} />
          <span>Just now</span>
        </div>
      </div>
    </div>
  );
};

export default AudienceSummaryPanel;

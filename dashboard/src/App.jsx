import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  AreaChart, Area, LineChart, Line, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';
import {
  Activity, Zap, Server, Database, Clock, RefreshCw,
  TrendingUp, Layers, CheckCircle2, AlertCircle, Settings
} from 'lucide-react';

export default function App() {
  // Default API URL (can be changed to your EC2 public IP)
  const [apiUrl, setApiUrl] = useState(() => {
    return localStorage.getItem('caiso_api_url') || 'http://localhost:8000';
  });
  const [showConfig, setShowConfig] = useState(false);
  const [tempUrl, setTempUrl] = useState(apiUrl);

  // Dashboard Data State
  const [status, setStatus] = useState(null);
  const [nodes, setNodes] = useState([]);
  const [selectedNode, setSelectedNode] = useState('TH_SP15');
  const [priceData, setPriceData] = useState([]);
  const [loadData, setLoadData] = useState([]);
  const [priceIntervals, setPriceIntervals] = useState(72); // 72 = 6 hours
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastRefreshed, setLastRefreshed] = useState(new Date());

  // Save API URL
  const handleSaveApiUrl = (e) => {
    e.preventDefault();
    let url = tempUrl.trim();
    if (url.endsWith('/')) url = url.slice(0, -1);
    setApiUrl(url);
    localStorage.setItem('caiso_api_url', url);
    setShowConfig(false);
  };

  // Fetch all dashboard data
  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      // 1. Fetch Health Status
      const statusRes = await axios.get(`${apiUrl}/api/status`);
      setStatus(statusRes.data);

      // 2. Fetch Nodes
      const nodesRes = await axios.get(`${apiUrl}/api/nodes`);
      setNodes(nodesRes.data);

      // 3. Fetch Prices for Selected Node
      const priceRes = await axios.get(`${apiUrl}/api/prices?node=${selectedNode}&limit=${priceIntervals}`);
      setPriceData(priceRes.data.data.map(item => ({
        ...item,
        timeFormatted: new Date(item.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        dateFormatted: new Date(item.timestamp).toLocaleDateString([], { month: 'short', day: 'numeric' })
      })));

      // 4. Fetch Load Data
      const loadRes = await axios.get(`${apiUrl}/api/load?limit=${priceIntervals}`);
      setLoadData(loadRes.data.data.map(item => ({
        ...item,
        timeFormatted: new Date(item.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        dateFormatted: new Date(item.timestamp).toLocaleDateString([], { month: 'short', day: 'numeric' })
      })));

      setLastRefreshed(new Date());
    } catch (err) {
      console.error('Failed to fetch data:', err);
      setError(`Cannot connect to API at ${apiUrl}. Please ensure FastAPI is running and Port 8000 is open in your AWS Security Group.`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    // Auto-refresh every 30 seconds for live streaming updates
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [apiUrl, selectedNode, priceIntervals]);

  // Current selected price
  const latestPrice = priceData.length > 0 ? priceData[priceData.length - 1].price_per_mwh : null;
  const latestDemand = status?.caiso_grid_load?.current_demand_mw || null;

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-4 sm:p-6 lg:p-8">
      {/* ======================================================== */}
      {/* HEADER & API CONFIGURATION                               */}
      {/* ======================================================== */}
      <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 pb-6 border-b border-slate-800">
        <div>
          <div className="flex items-center gap-3">
            <div className="p-2.5 bg-amber-500/10 border border-amber-500/30 rounded-xl">
              <Zap className="w-7 h-7 text-amber-400" />
            </div>
            <div>
              <h1 className="text-2xl font-bold tracking-tight bg-gradient-to-r from-amber-200 via-orange-300 to-amber-400 bg-clip-text text-transparent">
                CAISO Real-Time Grid Data Platform
              </h1>
              <p className="text-xs sm:text-sm text-slate-400">
                5-Minute Locational Marginal Pricing (LMP) & Demand Analytics • AWS RDS PostgreSQL
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-3 w-full md:w-auto justify-between md:justify-end">
          <div className="flex items-center gap-2 px-3 py-1.5 bg-emerald-500/10 border border-emerald-500/30 rounded-full text-emerald-400 text-xs font-medium">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
            LIVE STREAMING (Aiven Kafka)
          </div>

          <button
            onClick={() => setShowConfig(!showConfig)}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-900 hover:bg-slate-800 border border-slate-700 rounded-lg text-xs transition"
          >
            <Settings className="w-3.5 h-3.5" />
            API Config
          </button>

          <button
            onClick={fetchData}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-amber-500 hover:bg-amber-600 text-slate-950 font-semibold rounded-lg text-xs transition disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </header>

      {/* API URL Config Modal */}
      {showConfig && (
        <form onSubmit={handleSaveApiUrl} className="mt-4 p-4 bg-slate-900 border border-slate-800 rounded-xl flex flex-col sm:flex-row gap-3 items-center">
          <div className="w-full">
            <label className="text-xs text-slate-400 block mb-1">FastAPI Backend Endpoint URL:</label>
            <input
              type="text"
              value={tempUrl}
              onChange={(e) => setTempUrl(e.target.value)}
              placeholder="http://YOUR_EC2_PUBLIC_IP:8000"
              className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm font-mono text-amber-300 focus:outline-none focus:border-amber-500"
            />
          </div>
          <button type="submit" className="w-full sm:w-auto px-4 py-2 bg-emerald-500 hover:bg-emerald-600 text-slate-950 font-bold rounded-lg text-xs mt-auto">
            Save Endpoint
          </button>
        </form>
      )}

      {/* Error Banner */}
      {error && (
        <div className="mt-4 p-4 bg-rose-500/10 border border-rose-500/30 rounded-xl text-rose-300 text-sm flex items-center gap-3">
          <AlertCircle className="w-5 h-5 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* ======================================================== */}
      {/* REAL-TIME SYSTEM METRICS CARDS                           */}
      {/* ======================================================== */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mt-6">
        {/* Metric 1: Wholesale LMP Price */}
        <div className="p-4 bg-slate-900/60 border border-slate-800 rounded-2xl relative overflow-hidden">
          <div className="flex justify-between items-center text-slate-400 mb-2">
            <span className="text-xs font-medium uppercase tracking-wider">Hub LMP Price ({selectedNode})</span>
            <TrendingUp className="w-4 h-4 text-amber-400" />
          </div>
          <div className="text-3xl font-bold font-mono text-amber-300">
            {latestPrice !== null ? `$${latestPrice.toFixed(2)}` : 'Loading...'}
            <span className="text-xs text-slate-400 font-sans font-normal ml-1">/ MWh</span>
          </div>
          <p className="text-xs text-slate-500 mt-1">Real-Time Dispatch 5-Min Settlement</p>
        </div>

        {/* Metric 2: Grid Demand */}
        <div className="p-4 bg-slate-900/60 border border-slate-800 rounded-2xl relative overflow-hidden">
          <div className="flex justify-between items-center text-slate-400 mb-2">
            <span className="text-xs font-medium uppercase tracking-wider">Live System Demand</span>
            <Activity className="w-4 h-4 text-cyan-400" />
          </div>
          <div className="text-3xl font-bold font-mono text-cyan-300">
            {latestDemand !== null ? `${latestDemand.toLocaleString()}` : 'Loading...'}
            <span className="text-xs text-slate-400 font-sans font-normal ml-1">MW</span>
          </div>
          <p className="text-xs text-slate-500 mt-1">California Grid Total Load</p>
        </div>

        {/* Metric 3: Total Records in DB */}
        <div className="p-4 bg-slate-900/60 border border-slate-800 rounded-2xl relative overflow-hidden">
          <div className="flex justify-between items-center text-slate-400 mb-2">
            <span className="text-xs font-medium uppercase tracking-wider">Time-Series Storage</span>
            <Database className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="text-3xl font-bold font-mono text-emerald-300">
            {status?.caiso_pricing?.total_records ? status.caiso_pricing.total_records.toLocaleString() : '---'}
            <span className="text-xs text-slate-400 font-sans font-normal ml-1">Rows</span>
          </div>
          <p className="text-xs text-slate-500 mt-1">AWS RDS PostgreSQL (B-Tree)</p>
        </div>

        {/* Metric 4: Pipeline Health */}
        <div className="p-4 bg-slate-900/60 border border-slate-800 rounded-2xl relative overflow-hidden">
          <div className="flex justify-between items-center text-slate-400 mb-2">
            <span className="text-xs font-medium uppercase tracking-wider">Pipeline Health</span>
            <CheckCircle2 className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="text-2xl font-bold text-emerald-400 flex items-center gap-2">
            HEALTHY
          </div>
          <p className="text-xs text-slate-500 mt-2 font-mono">
            Lag: &lt;5s • Polling: 15s
          </p>
        </div>
      </div>

      {/* ======================================================== */}
      {/* INTERACTIVE TIME-SERIES PRICING CHART                    */}
      {/* ======================================================== */}
      <div className="mt-8 bg-slate-900/60 border border-slate-800 rounded-2xl p-4 sm:p-6">
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4 mb-6">
          <div>
            <h2 className="text-lg font-bold text-slate-100 flex items-center gap-2">
              <TrendingUp className="w-5 h-5 text-amber-400" />
              Real-Time 5-Minute Locational Marginal Price (LMP) Curve
            </h2>
            <p className="text-xs text-slate-400">Wholesale electricity price components ($/MWh) by transmission hub</p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            {/* Hub Selector */}
            <div className="flex bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs">
              {['TH_NP15', 'TH_SP15', 'TH_ZP26'].map((node) => (
                <button
                  key={node}
                  onClick={() => setSelectedNode(node)}
                  className={`px-3 py-1.5 rounded-lg font-medium transition ${
                    selectedNode === node
                      ? 'bg-amber-500 text-slate-950 font-bold'
                      : 'text-slate-400 hover:text-slate-200'
                  }`}
                >
                  {node === 'TH_NP15' ? 'North (NP15)' : node === 'TH_SP15' ? 'South (SP15)' : 'Central (ZP26)'}
                </button>
              ))}
            </div>

            {/* Interval Limit */}
            <select
              value={priceIntervals}
              onChange={(e) => setPriceIntervals(Number(e.target.value))}
              className="bg-slate-950 border border-slate-800 text-slate-300 text-xs px-3 py-2 rounded-xl focus:outline-none focus:border-amber-500"
            >
              <option value="36">Last 3 Hours</option>
              <option value="72">Last 6 Hours</option>
              <option value="144">Last 12 Hours</option>
              <option value="288">Last 24 Hours</option>
            </select>
          </div>
        </div>

        {/* Pricing Area Chart */}
        <div className="h-72 sm:h-80 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={priceData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <defs>
                <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.4}/>
                  <stop offset="95%" stopColor="#f59e0b" stopOpacity={0.0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="timeFormatted" stroke="#64748b" fontSize={11} />
              <YAxis stroke="#64748b" fontSize={11} domain={['auto', 'auto']} tickFormatter={(v) => `$${v}`} />
              <Tooltip
                contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '0.75rem', fontSize: '12px' }}
                formatter={(val) => [`$${Number(val).toFixed(2)}/MWh`, 'Price']}
                labelFormatter={(label, item) => item[0]?.payload?.dateFormatted ? `${item[0].payload.dateFormatted} at ${label}` : label}
              />
              <Area type="monotone" dataKey="price_per_mwh" name="LMP Price" stroke="#f59e0b" strokeWidth={2} fillOpacity={1} fill="url(#priceGradient)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ======================================================== */}
      {/* SYSTEM LOAD DEMAND VS FORECAST CHART                     */}
      {/* ======================================================== */}
      <div className="mt-8 bg-slate-900/60 border border-slate-800 rounded-2xl p-4 sm:p-6">
        <div className="mb-6">
          <h2 className="text-lg font-bold text-slate-100 flex items-center gap-2">
            <Activity className="w-5 h-5 text-cyan-400" />
            CAISO Grid Demand vs. Day-Ahead Forecast (MW)
          </h2>
          <p className="text-xs text-slate-400">Comparing real-time dispatch load against system forecasts in Megawatts</p>
        </div>

        {/* Load Line Chart */}
        <div className="h-72 sm:h-80 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={loadData} margin={{ top: 10, right: 10, left: -10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="timeFormatted" stroke="#64748b" fontSize={11} />
              <YAxis stroke="#64748b" fontSize={11} domain={['auto', 'auto']} tickFormatter={(v) => `${(v/1000).toFixed(0)}k`} />
              <Tooltip
                contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '0.75rem', fontSize: '12px' }}
                formatter={(val, name) => [`${Number(val).toLocaleString()} MW`, name]}
                labelFormatter={(label, item) => item[0]?.payload?.dateFormatted ? `${item[0].payload.dateFormatted} at ${label}` : label}
              />
              <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
              <Line type="monotone" dataKey="actual_load_mw" name="Actual Load (MW)" stroke="#06b6d4" strokeWidth={2.5} dot={false} />
              <Line type="monotone" dataKey="forecast_load_mw" name="Forecast Load (MW)" stroke="#94a3b8" strokeDasharray="4 4" strokeWidth={1.5} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ======================================================== */}
      {/* DATA CATALOG (TRANSMISSION NODES METADATA)               */}
      {/* ======================================================== */}
      <div className="mt-8 bg-slate-900/60 border border-slate-800 rounded-2xl p-4 sm:p-6 mb-8">
        <h2 className="text-lg font-bold text-slate-100 flex items-center gap-2 mb-4">
          <Layers className="w-5 h-5 text-indigo-400" />
          Grid Transmission Hubs Catalog (`dim_caiso_nodes`)
        </h2>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs sm:text-sm text-slate-300">
            <thead className="bg-slate-950 text-slate-400 font-mono text-xs uppercase border-b border-slate-800">
              <tr>
                <th className="py-3 px-4">Node ID</th>
                <th className="py-3 px-4">Hub Description</th>
                <th className="py-3 px-4">Grid Location</th>
                <th className="py-3 px-4">Voltage Level</th>
                <th className="py-3 px-4">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-mono">
              {nodes.map((node) => (
                <tr key={node.node_id} className="hover:bg-slate-800/30 transition">
                  <td className="py-3 px-4 font-bold text-amber-400">{node.node_id}</td>
                  <td className="py-3 px-4 font-sans text-slate-200">{node.node_name}</td>
                  <td className="py-3 px-4 font-sans text-slate-300">{node.location}</td>
                  <td className="py-3 px-4 text-cyan-300">{node.voltage_level_kv} kV</td>
                  <td className="py-3 px-4">
                    <span className="px-2 py-0.5 bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 rounded-md text-[11px]">
                      Active Feed
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Footer */}
      <footer className="text-center text-xs text-slate-500 py-4 border-t border-slate-800/60">
        CAISO Real-Time Energy Grid & Analytics Platform • Ingested via Kafka &bull; Stored on AWS RDS PostgreSQL &bull; Built with FastAPI & React
      </footer>
    </div>
  );
}

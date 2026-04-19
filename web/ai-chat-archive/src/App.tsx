import React, { useState, useMemo, useEffect, useRef } from 'react';
import { Search, MessageSquare, Clock, Cpu, Database, Server, Zap, Menu, X, ChevronUp, ChevronDown } from 'lucide-react';
import { format } from 'date-fns';
import { cn } from './lib/utils';
import { api, Session, SearchHit } from './lib/mockData';

export default function App() {
  const [searchQuery, setSearchQuery] = useState('');
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  
  // Data state
  const [sessions, setSessions] = useState<Session[]>([]);
  
  // Search state
  const [searchHits, setSearchHits] = useState<SearchHit[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [searchTookMs, setSearchTookMs] = useState(0);

  // Active session and highlight state
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null);
  const [currentHitIdx, setCurrentHitIdx] = useState(0);

  const pairRefs = useRef<Record<number, HTMLDivElement | null>>({});

  // Initial Load
  useEffect(() => {
    const loadInitData = async () => {
      const res = await api.init();
      setSessions(res.sessions);
      if (res.sessions.length > 0) {
        setActiveSessionId(res.sessions[0].meta_data.session_id);
      }
    };
    loadInitData();
  }, []);

  // Handle Search
  useEffect(() => {
    if (!searchQuery.trim()) {
      setIsSearching(false);
      setSearchHits([]);
      // When clearing search, we should ideally reload init data or keep the current sessions.
      // For this mock, we just clear the search state.
      return;
    }
    
    const delay = setTimeout(async () => {
      setIsSearching(true);
      const res = await api.search(searchQuery);
      setSearchHits(res.hits);
      setSessions(res.sessions); // Update sidebar with only matched sessions
      setSearchTookMs(res.took_ms);
      
      if (res.hits.length > 0 && !res.hits.find(h => h.session_id === activeSessionId)) {
        setActiveSessionId(res.hits[0].session_id);
      }
    }, 300); // debounce

    return () => clearTimeout(delay);
  }, [searchQuery]);

  const activeSession = useMemo(() => {
    return sessions.find(s => s.meta_data.session_id === activeSessionId) || null;
  }, [activeSessionId, sessions]);

  const activeHits = useMemo(() => {
    return searchHits.find(h => h.session_id === activeSessionId)?.matched_pair_indices || [];
  }, [searchHits, activeSessionId]);

  // Handle smooth scrolling to highlights
  useEffect(() => {
    if (isSearching && activeHits.length > 0) {
      setCurrentHitIdx(0);
      scrollToPair(activeHits[0]);
    }
  }, [activeSessionId, isSearching, activeHits]);

  const scrollToPair = (pairIndex: number) => {
    const el = pairRefs.current[pairIndex];
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
  };

  const nextHit = () => {
    if (activeHits.length === 0) return;
    const nextIdx = (currentHitIdx + 1) % activeHits.length;
    setCurrentHitIdx(nextIdx);
    scrollToPair(activeHits[nextIdx]);
  };

  const prevHit = () => {
    if (activeHits.length === 0) return;
    const prevIdx = (currentHitIdx - 1 + activeHits.length) % activeHits.length;
    setCurrentHitIdx(prevIdx);
    scrollToPair(activeHits[prevIdx]);
  };

  return (
    <div className="flex h-screen w-full bg-[#0a0a0a] text-gray-200 font-sans overflow-hidden selection:bg-emerald-500/30">
      
      {/* Mobile Sidebar Overlay */}
      {!isSidebarOpen && (
        <button 
          className="md:hidden fixed top-4 left-4 z-50 p-2 bg-gray-800 rounded-md text-gray-300"
          onClick={() => setIsSidebarOpen(true)}
        >
          <Menu size={20} />
        </button>
      )}

      {/* Sidebar */}
      <aside 
        className={cn(
          "fixed md:static inset-y-0 left-0 z-40 w-80 bg-[#111111] border-r border-gray-800 flex flex-col transition-transform duration-300 ease-in-out",
          isSidebarOpen ? "translate-x-0" : "-translate-x-full md:translate-x-0"
        )}
      >
        {/* Sidebar Header */}
        <div className="p-4 border-b border-gray-800 flex items-center justify-between">
          <div className="flex items-center gap-2 text-emerald-400 font-semibold">
            <Database size={20} />
            <span>KV Chat Archive</span>
          </div>
          <button 
            className="md:hidden text-gray-400 hover:text-white"
            onClick={() => setIsSidebarOpen(false)}
          >
            <X size={20} />
          </button>
        </div>

        {/* Search Box */}
        <div className="p-4 border-b border-gray-800">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={16} />
            <input 
              type="text" 
              placeholder="Search conversations..." 
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value);
                if (e.target.value === '') {
                  // Reload init data if search is cleared
                  api.init().then(res => setSessions(res.sessions));
                }
              }}
              className="w-full bg-gray-900 border border-gray-700 rounded-md py-2 pl-9 pr-4 text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-emerald-500 focus:ring-1 focus:ring-emerald-500 transition-all"
            />
          </div>
          {isSearching && (
            <div className="mt-2 text-xs text-gray-500 flex items-center gap-1">
              <Zap size={12} className="text-emerald-500" />
              Found {searchHits.length} sessions in {searchTookMs}ms
            </div>
          )}
        </div>

        {/* Conversation List */}
        <div className="flex-1 overflow-y-auto p-2 space-y-1 custom-scrollbar">
          {sessions.length === 0 ? (
            <div className="text-center text-gray-500 text-sm py-8">
              No conversations found.
            </div>
          ) : (
            sessions.map((session) => {
              const meta = session.meta_data;
              const isActive = activeSessionId === meta.session_id;
              const hitCount = searchHits.find(h => h.session_id === meta.session_id)?.matched_pair_indices.length || 0;
              
              return (
                <button
                  key={meta.session_id}
                  onClick={() => {
                    setActiveSessionId(meta.session_id);
                    if (window.innerWidth < 768) setIsSidebarOpen(false);
                  }}
                  className={cn(
                    "w-full text-left p-3 rounded-md transition-colors flex flex-col gap-1.5",
                    isActive 
                      ? "bg-gray-800/80 border border-gray-700" 
                      : "hover:bg-gray-800/40 border border-transparent"
                  )}
                >
                  <div className="font-medium text-sm text-gray-200 truncate flex justify-between items-center">
                    <span className="truncate pr-2">{meta.title}</span>
                    {isSearching && hitCount > 0 && (
                      <span className="bg-emerald-500/20 text-emerald-400 text-[10px] px-1.5 py-0.5 rounded-full shrink-0">
                        {hitCount} hits
                      </span>
                    )}
                  </div>
                  <div className="flex items-center justify-between text-xs text-gray-500">
                    <span className="flex items-center gap-1">
                      <Cpu size={12} />
                      {meta.model}
                    </span>
                    <span className="flex items-center gap-1">
                      <Clock size={12} />
                      {format(meta.timestamp, 'MMM d, HH:mm')}
                    </span>
                  </div>
                </button>
              );
            })
          )}
        </div>
        
        {/* System Status Footer */}
        <div className="p-4 border-t border-gray-800 text-xs text-gray-500 flex flex-col gap-2 bg-[#0d0d0d]">
          <div className="flex items-center justify-between">
            <span className="flex items-center gap-1"><Server size={12} /> Engine Status</span>
            <span className="text-emerald-400 flex items-center gap-1">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></span> Online
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span className="flex items-center gap-1"><Cpu size={12} /> io_uring Workers</span>
            <span className="text-gray-300">16 Threads</span>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col min-w-0 bg-[#0a0a0a] relative">
        {activeSession ? (
          <>
            {/* Header */}
            <header className="h-16 border-b border-gray-800 flex items-center justify-between px-6 md:px-8 shrink-0 bg-[#0a0a0a]/80 backdrop-blur-sm sticky top-0 z-10">
              <div className="md:ml-0 ml-10">
                <h1 className="text-lg font-semibold text-gray-100">{activeSession.meta_data.title}</h1>
                <div className="text-xs text-gray-500 flex items-center gap-2 mt-0.5">
                  <span>ID: {activeSession.meta_data.session_id}</span>
                  <span>•</span>
                  <span>{activeSession.meta_data.model}</span>
                </div>
              </div>
            </header>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto p-4 md:p-8 space-y-8 custom-scrollbar pb-32">
              {activeSession.pairs.map((pair) => {
                const isHighlighted = isSearching && activeHits.includes(pair.pair_index);
                const isActiveHighlight = isHighlighted && activeHits[currentHitIdx] === pair.pair_index;

                return (
                  <div 
                    key={pair.pair_index}
                    ref={(el) => { pairRefs.current[pair.pair_index] = el; }}
                    className={cn(
                      "flex flex-col gap-6 max-w-3xl mx-auto p-4 rounded-xl transition-all duration-500",
                      isHighlighted ? "bg-emerald-900/10 border border-emerald-500/20" : "border border-transparent",
                      isActiveHighlight ? "ring-1 ring-emerald-500/50 shadow-[0_0_15px_rgba(16,185,129,0.1)]" : ""
                    )}
                  >
                    {/* User Message */}
                    <div className="flex flex-col items-end">
                      <div className="flex items-center gap-2 mb-1.5 px-1">
                        <span className="text-xs font-medium text-gray-400">You</span>
                      </div>
                      <div className="px-4 py-3 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap shadow-sm bg-emerald-600/20 text-emerald-50 border border-emerald-500/20 rounded-tr-sm">
                        {pair.user_msg.content}
                      </div>
                    </div>

                    {/* AI Message */}
                    <div className="flex flex-col items-start">
                      <div className="flex items-center gap-2 mb-1.5 px-1">
                        <span className="text-xs font-medium text-gray-400">AI Assistant</span>
                      </div>
                      <div className="px-4 py-3 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap shadow-sm bg-gray-800/50 text-gray-200 border border-gray-700/50 rounded-tl-sm">
                        {pair.ai_msg.content}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* Floating Highlight Navigation */}
            {isSearching && activeHits.length > 0 && (
              <div className="absolute bottom-8 left-1/2 -translate-x-1/2 flex items-center gap-3 bg-gray-800/90 backdrop-blur-md px-4 py-2 rounded-full shadow-2xl border border-gray-700 z-20">
                <span className="text-sm font-medium text-gray-300">
                  <span className="text-emerald-400">{currentHitIdx + 1}</span> / {activeHits.length} matches
                </span>
                <div className="w-px h-4 bg-gray-600 mx-1"></div>
                <button 
                  onClick={prevHit}
                  className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-700 rounded-full transition-colors"
                  title="Previous match"
                >
                  <ChevronUp size={18} />
                </button>
                <button 
                  onClick={nextHit}
                  className="p-1.5 text-gray-400 hover:text-white hover:bg-gray-700 rounded-full transition-colors"
                  title="Next match"
                >
                  <ChevronDown size={18} />
                </button>
              </div>
            )}
          </>
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center text-gray-500">
            <MessageSquare size={48} className="mb-4 opacity-20" />
            <p>Select a conversation to view the archive</p>
          </div>
        )}
      </main>
    </div>
  );
}

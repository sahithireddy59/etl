import React, { useCallback, useState } from 'react';
import ReactFlow, {
  MiniMap, Controls, Background, applyNodeChanges, applyEdgeChanges, Handle
} from 'reactflow';
import 'reactflow/dist/style.css';

const CustomNode = (props) => {
  const { data } = props;
  return (
    <div style={{ padding: 10, border: '1px solid #888', borderRadius: 5, background: '#f8f8ff', minWidth: 180 }}>
      <strong>{data.label}</strong>
      {data.type === 'expression' && (
        <ul style={{ paddingLeft: 16 }}>
          {data.expressions && data.expressions.length > 0
            ? data.expressions.map((expr, idx) => (
                <li key={idx}><b>{expr.column}</b>: {expr.expression}</li>
              ))
            : <li style={{ color: '#aaa' }}>No expressions</li>
          }
        </ul>
      )}
      {data.type === 'filter' && (
        <div style={{ color: '#555', marginTop: 6 }}>
          {data.condition || <span style={{ color: '#aaa' }}>No condition</span>}
        </div>
      )}
      {data.type === 'rollup' && (
        <div style={{ fontSize: 13, marginTop: 6 }}>
          <b>Group by:</b> {data.groupBy?.join(', ') || <span style={{ color: '#aaa' }}>None</span>}
          <br />
          <b>Aggregations:</b> {data.aggregations
            ? Object.entries(data.aggregations).map(([col, agg]) => `${agg}(${col})`).join(', ')
            : <span style={{ color: '#aaa' }}>None</span>}
        </div>
      )}
      <Handle type="target" position="top" />
      <Handle type="source" position="bottom" />
    </div>
  );
};

const nodeTypes = { custom: CustomNode };

const initialNodes = [
  { id: '1', type: 'custom', data: { label: 'Source Table', type: 'input' }, position: { x: 0, y: 100 } },
  { id: '2', type: 'custom', data: { label: 'Target Table', type: 'output' }, position: { x: 500, y: 100 } },
];

const initialEdges = [];

export default function App() {
  const [nodes, setNodes] = useState(initialNodes);
  const [edges, setEdges] = useState(initialEdges);
  const [selectedNode, setSelectedNode] = useState(null);
  const [labelInput, setLabelInput] = useState('');
  const [expressions, setExpressions] = useState([]);
  const [filterCondition, setFilterCondition] = useState('');
  const [groupBy, setGroupBy] = useState([]);
  const [aggregations, setAggregations] = useState({});
  const [showHelp, setShowHelp] = useState(false);
  const [showSimpleCopy, setShowSimpleCopy] = useState(false);
  const [simpleCopyName, setSimpleCopyName] = useState('');
  const [simpleCopySource, setSimpleCopySource] = useState('');
  const [simpleCopyTarget, setSimpleCopyTarget] = useState('');
  const [aggInput, setAggInput] = useState('');

  // Add node functions
  const addExpressionNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: { label: 'Expression', type: 'expression', expressions: [] },
        position: { x: 200 + nds.length * 40, y: 200 },
      },
    ]);
  };
  const addFilterNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: { label: 'Filter', type: 'filter', condition: '' },
        position: { x: 200 + nds.length * 40, y: 300 },
      },
    ]);
  };
  const addRollupNode = () => {
    const newId = (nodes.length + 1).toString();
    setNodes(nds => [
      ...nds,
      {
        id: newId,
        type: 'custom',
        data: { label: 'Rollup', type: 'rollup', groupBy: [], aggregations: {} },
        position: { x: 200 + nds.length * 40, y: 400 },
      },
    ]);
  };

  const onNodesChange = useCallback(
    (changes) => setNodes((nds) => applyNodeChanges(changes, nds)),
    []
  );
  const onEdgesChange = useCallback(
    (changes) => setEdges((eds) => applyEdgeChanges(changes, eds)),
    []
  );
  const onConnect = useCallback(
    (params) => setEdges((eds) => [...eds, { ...params, id: `${params.source}-${params.target}` }]),
    []
  );
  const onEdgeClick = (event, edge) => {
    event.stopPropagation();
    setEdges(eds => eds.filter(e => e.id !== edge.id));
  };

  // Double-click node to edit
  const onNodeDoubleClick = (event, node) => {
    setSelectedNode(node);
    setLabelInput(node.data.label);
    if (node.data.type === 'expression') {
      setExpressions(node.data.expressions || []);
    } else if (node.data.type === 'filter') {
      setFilterCondition(node.data.condition || '');
    } else if (node.data.type === 'rollup') {
      setGroupBy(node.data.groupBy || []);
      setAggregations(node.data.aggregations || {});
      setAggInput(
        node.data.aggregations
          ? Object.entries(node.data.aggregations).map(([col, agg]) => `${col}:${agg}`).join(',')
          : ''
      );
    }
  };

  // Save node edits
  const handleSave = () => {
    let aggs = aggregations;
    if (selectedNode.data.type === 'rollup') {
      // Parse aggInput on save
      aggs = {};
      aggInput.split(',').forEach(pair => {
        const [col, agg] = pair.split(':').map(s => s.trim());
        if (col && agg) aggs[col] = agg;
      });
    }
    setNodes(nds =>
      nds.map(n =>
        n.id === selectedNode.id
          ? {
              ...n,
              data: {
                ...n.data,
                label: labelInput,
                ...(selectedNode.data.type === 'expression' ? { expressions } : {}),
                ...(selectedNode.data.type === 'filter' ? { condition: filterCondition } : {}),
                ...(selectedNode.data.type === 'rollup' ? { groupBy, aggregations: aggs } : {}),
              },
            }
          : n
      )
    );
    setSelectedNode(null);
  };

  // Delete node and rewire edges
  const deleteNodeAndRewire = (nodeId) => {
    setNodes(nds => nds.filter(n => n.id !== nodeId));
    setEdges(eds => {
      const incoming = eds.filter(e => e.target === nodeId);
      const outgoing = eds.filter(e => e.source === nodeId);
      let newEdges = eds.filter(e => e.source !== nodeId && e.target !== nodeId);
      incoming.forEach(inEdge => {
        outgoing.forEach(outEdge => {
          if (inEdge.source !== outEdge.target && !newEdges.some(e => e.source === inEdge.source && e.target === outEdge.target)) {
            newEdges.push({
              id: `e${inEdge.source}-${outEdge.target}`,
              source: inEdge.source,
              target: outEdge.target,
            });
          }
        });
      });
      return newEdges;
    });
    setSelectedNode(null);
  };

  // Expression editing helpers
  const handleExprChange = (idx, field, value) => {
    setExpressions(exprs => {
      const updated = [...exprs];
      updated[idx][field] = value;
      return updated;
    });
  };
  const addExprRow = () => setExpressions(exprs => [...exprs, { column: '', expression: '' }]);
  const removeExprRow = idx => setExpressions(exprs => exprs.filter((_, i) => i !== idx));

  const savePipeline = async () => {
    const pipeline = { nodes, edges };
    try {
      const response = await fetch('http://localhost:8000/create/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pipeline),
      });
      if (response.ok) {
        alert('Pipeline saved and Airflow triggered!');
      } else {
        alert('Failed to save pipeline.');
      }
    } catch (error) {
      alert('Error: ' + error);
    }
  };

  const handleSimpleCopy = async () => {
    if (!simpleCopyName || !simpleCopySource || !simpleCopyTarget) {
      alert('Please fill all fields.');
      return;
    }
    const payload = {
      name: simpleCopyName,
      nodes: [
        { id: '1', type: 'custom', data: { type: 'input', label: simpleCopySource } },
        { id: '2', type: 'custom', data: { type: 'output', label: simpleCopyTarget } }
      ],
      edges: []
    };
    try {
      const response = await fetch('http://localhost:8000/create/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (response.ok) {
        alert('Simple copy job saved and Airflow triggered!');
        setShowSimpleCopy(false);
        setSimpleCopyName('');
        setSimpleCopySource('');
        setSimpleCopyTarget('');
      } else {
        alert('Failed to save simple copy job.');
      }
    } catch (error) {
      alert('Error: ' + error);
    }
  };

  return (
    <div>
      <div style={{ height: 500 }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onEdgeClick={onEdgeClick}
          onNodeDoubleClick={onNodeDoubleClick}
          fitView
          nodeTypes={nodeTypes}
        >
          <MiniMap />
          <Controls />
          <Background />
        </ReactFlow>
        <div style={{ marginTop: 20 }}>
          <button onClick={addExpressionNode} style={{ marginRight: 10 }}>Add Expression Node</button>
          <button onClick={addFilterNode} style={{ marginRight: 10 }}>Add Filter Node</button>
          <button onClick={addRollupNode} style={{ marginRight: 10 }}>Add Rollup Node</button>
          <button onClick={savePipeline} style={{ marginLeft: 10 }}>Save Pipeline</button>
          <button onClick={() => setShowSimpleCopy(true)} style={{ marginLeft: 10, background: '#2ecc40', color: '#fff' }}>Simple Copy</button>
          <button onClick={() => setShowHelp(h => !h)} style={{ marginLeft: 10 }}>Help</button>
          <span style={{ marginLeft: 20, color: '#888' }}>
            You can add and connect nodes by dragging from the bottom handle to the top handle of another node.
          </span>
        </div>
        {/* Simple Copy Dialog */}
        {showSimpleCopy && (
          <div style={{ position: 'fixed', top: '30%', left: '35%', background: '#fff', padding: 24, border: '1px solid #888', zIndex: 20, minWidth: 400 }}>
            <h3>Simple Table Copy</h3>
            <div style={{ marginBottom: 10 }}>
              <label>Job Name:</label>
              <input value={simpleCopyName} onChange={e => setSimpleCopyName(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ marginBottom: 10 }}>
              <label>Source Table:</label>
              <input value={simpleCopySource} onChange={e => setSimpleCopySource(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ marginBottom: 10 }}>
              <label>Target Table:</label>
              <input value={simpleCopyTarget} onChange={e => setSimpleCopyTarget(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ marginTop: 16 }}>
              <button onClick={handleSimpleCopy} style={{ marginRight: 10 }}>Save</button>
              <button onClick={() => setShowSimpleCopy(false)}>Cancel</button>
            </div>
          </div>
        )}
        {/* Node editing dialogs */}
        {selectedNode && (
          <div style={{ position: 'fixed', top: '25%', left: '35%', background: '#fff', padding: 24, border: '1px solid #888', zIndex: 20, minWidth: 400 }}>
            <h3>Edit Node</h3>
            <div>
              <label>Label:</label>
              <input
                value={labelInput}
                onChange={e => setLabelInput(e.target.value)}
                style={{ width: '100%', marginBottom: 10 }}
              />
            </div>
            {selectedNode.data.type === 'expression' && (
              <div>
                <h4>Expressions</h4>
                {expressions.map((expr, idx) => (
                  <div key={idx} style={{ display: 'flex', gap: 8, marginBottom: 6 }}>
                    <input
                      placeholder="Output Column"
                      value={expr.column}
                      onChange={e => handleExprChange(idx, 'column', e.target.value)}
                      style={{ width: 120 }}
                    />
                    <input
                      placeholder="Expression"
                      value={expr.expression}
                      onChange={e => handleExprChange(idx, 'expression', e.target.value)}
                      style={{ width: 200 }}
                    />
                    <button onClick={() => removeExprRow(idx)} style={{ color: 'red' }}>Remove</button>
                  </div>
                ))}
                <button onClick={addExprRow} style={{ marginTop: 8 }}>Add Expression</button>
              </div>
            )}
            {selectedNode.data.type === 'filter' && (
              <div>
                <label>Condition:</label>
                <input
                  placeholder="e.g. count_st > 10"
                  value={filterCondition}
                  onChange={e => setFilterCondition(e.target.value)}
                  style={{ width: '100%', marginBottom: 10 }}
                />
              </div>
            )}
            {selectedNode.data.type === 'rollup' && (
              <div>
                <label>Group By (comma separated):</label>
                <input
                  value={groupBy.join(',')}
                  onChange={e => setGroupBy(e.target.value.split(',').map(s => s.trim()).filter(Boolean))}
                  style={{ width: '100%', marginBottom: 10 }}
                />
                <label>Aggregations (format: col:agg, e.g. count_st:sum):</label>
                <input
                  value={aggInput}
                  onChange={e => setAggInput(e.target.value)}
                  style={{ width: '100%', marginBottom: 10 }}
                />
              </div>
            )}
            <div style={{ marginTop: 16 }}>
              <button onClick={handleSave}>Save</button>
              {/* Only allow delete for non-source/target nodes */}
              {selectedNode.data.type !== 'input' && selectedNode.data.type !== 'output' && (
                <button onClick={() => deleteNodeAndRewire(selectedNode.id)} style={{ marginLeft: 10, color: 'red' }}>Delete Node</button>
              )}
              <button onClick={() => setSelectedNode(null)} style={{ marginLeft: 10 }}>Cancel</button>
            </div>
          </div>
        )}
        {/* Help overlay */}
        {showHelp && (
          <div style={{ position: 'fixed', top: 60, left: '20%', width: 400, background: '#fff', border: '2px solid #0074D9', borderRadius: 8, padding: 20, zIndex: 1000 }}>
            <h3>How to use the ETL Visual Builder</h3>
            <ul>
              <li>To <b>connect nodes</b>: Drag from the <b>bottom handle</b> of one node to the <b>top handle</b> of another node.</li>
              <li>To <b>delete a connection</b>: Click on the link/edge between nodes.</li>
              <li>To <b>add a node</b>: Use the buttons below the canvas.</li>
              <li>To <b>edit or delete a node</b>: Double-click the node.</li>
              <li>To <b>save your pipeline</b>: Click "Save Pipeline".</li>
            </ul>
            <button onClick={() => setShowHelp(false)} style={{ marginTop: 10 }}>Close</button>
          </div>
        )}
      </div>
    </div>
  );
}
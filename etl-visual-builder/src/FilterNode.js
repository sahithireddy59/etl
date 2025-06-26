import React from 'react';
import { Handle } from 'reactflow';

export default function FilterNode({ data }) {
  return (
    <div style={{ padding: 10, border: '1px solid #888', borderRadius: 5, background: '#fffbe7', minWidth: 180 }}>
      <strong>Filter</strong>
      <div style={{ color: '#555', marginTop: 6 }}>
        {data.condition || <span style={{ color: '#aaa' }}>No condition</span>}
      </div>
      <Handle type="target" position="top" />
      <Handle type="source" position="bottom" />
    </div>
  );
} 
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Landing from './pages/Landing.jsx'
import Dashboard from './pages/Dashboard.jsx'
import KeyManager from './pages/KeyManager.jsx'
import ProductExplorer from './pages/ProductExplorer.jsx'
import MapView from './pages/MapView.jsx'
import AdminUsage from './pages/AdminUsage.jsx'
import SceneMetadata from './pages/SceneMetadata.jsx'
import PrivateRoute from './components/PrivateRoute.jsx'
import AdminRoute from './components/AdminRoute.jsx'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Landing />} />
        <Route path="/dashboard" element={<PrivateRoute><Dashboard /></PrivateRoute>} />
        <Route path="/keys" element={<PrivateRoute><KeyManager /></PrivateRoute>} />
        <Route path="/explore/:product" element={<PrivateRoute><ProductExplorer /></PrivateRoute>} />
        <Route path="/scene/:sceneId" element={<PrivateRoute><SceneMetadata /></PrivateRoute>} />
        <Route path="/map" element={<PrivateRoute><MapView /></PrivateRoute>} />
        <Route path="/admin" element={<AdminRoute><AdminUsage /></AdminRoute>} />
      </Routes>
    </BrowserRouter>
  )
}

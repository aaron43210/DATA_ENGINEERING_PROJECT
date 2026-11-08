import { useNavigate } from 'react-router-dom'
import { GoogleLogin } from '@react-oauth/google'
import { useAuth } from '../context/AuthContext.jsx'
import { api } from '../api/client.js'

export default function Landing() {
  const { login } = useAuth()
  const navigate = useNavigate()

  async function handleSuccess(response) {
    try {
      const data = await api.post('/auth/google', { credential: response.credential })
      login(data.token, data.user)
      navigate('/dashboard')
    } catch (err) {
      alert('Sign-in failed: ' + err.message)
    }
  }

  return (
    <div className="min-h-screen flex flex-col items-center justify-center bg-gray-950 text-white px-4">
      <div className="mb-8 text-center">
        <h1 className="text-4xl font-bold tracking-tight mb-2">Geospatial Data Platform</h1>
        <p className="text-gray-400 text-lg">Kerala · Cloud-Native · Real-Time</p>
      </div>
      <div className="bg-gray-900 rounded-2xl p-8 shadow-xl flex flex-col items-center gap-4 w-full max-w-sm">
        <p className="text-gray-300 text-sm mb-2">Sign in to access data products and manage API keys</p>
        <GoogleLogin onSuccess={handleSuccess} onError={() => alert('Google sign-in failed')} />
      </div>
    </div>
  )
}

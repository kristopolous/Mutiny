package net.mutiny.app

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.util.concurrent.TimeUnit

/**
 * Loads album art (the release dir's album-art.jpg) for the media notification.
 * Art is optional: many releases have none, so a 404 is cached as "no art".
 */
object Artwork {

    private val http = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .build()

    private val memory = HashMap<String, Bitmap?>()

    /** @return a bitmap, or null when the art does not exist / failed. */
    suspend fun load(ctx: Context, url: String): Bitmap? = withContext(Dispatchers.IO) {
        if (url.isBlank()) return@withContext null
        memory[url]?.let { return@withContext it }
        val dir = File(ctx.cacheDir, "art").apply { mkdirs() }
        val name = Integer.toHexString(url.hashCode())
        val file = File(dir, name)
        val miss = File(dir, name + ".404")

        var bmp: Bitmap? = null
        if (file.isFile && file.length() > 0) {
            bmp = BitmapFactory.decodeFile(file.absolutePath)
        } else if (miss.isFile) {
            bmp = null
        } else {
            try {
                val req = Request.Builder().url(url).build()
                http.newCall(req).execute().use { resp ->
                    if (resp.code == 200) {
                        resp.body?.byteStream()?.use { inp ->
                            file.outputStream().use { out -> inp.copyTo(out) }
                        }
                        bmp = BitmapFactory.decodeFile(file.absolutePath)
                        if (bmp == null) { file.delete(); miss.createNewFile() }
                    } else {
                        miss.createNewFile()
                    }
                }
            } catch (e: Exception) {
                // leave it for the next attempt
            }
        }
        val result: Bitmap? = bmp
        if (result == null) {
            memory[url] = null
            return@withContext null
        }
        // keep the notification icon small
        val scaled = scale(result, 512)
        memory[url] = scaled
        scaled
    }

    fun clear(url: String) {
        memory.remove(url)
    }

    private fun scale(bmp: Bitmap, max: Int): Bitmap {
        val s = minOf(1f, max.toFloat() / maxOf(bmp.width, bmp.height))
        if (s >= 1f) return bmp
        return Bitmap.createScaledBitmap(bmp, (bmp.width * s).toInt(), (bmp.height * s).toInt(), true)
    }
}
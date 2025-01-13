/**
 * Copyright (c) 2010-2024 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.core.internal.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.core.common.ThreadPoolManager;
import org.openhab.core.service.WatchService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.methvin.watcher.DirectoryChangeEvent;
import io.methvin.watcher.DirectoryChangeListener;
import io.methvin.watcher.DirectoryWatcher;
import io.methvin.watcher.hashing.FileHash;

@NonNullByDefault
@Component(immediate = true, service = WatchService.class, configurationPid = WatchService.SERVICE_PID, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class WatchServiceImpl implements WatchService, DirectoryChangeListener {

    private static final int EVENT_BATCH_SIZE = 100;
    private static final int EVENT_QUEUE_SIZE = 10000;
    private static final int HASH_CACHE_MAX_SIZE = 10000;
    private static final long BATCH_PROCESSING_INTERVAL = 1000;
    private static final long BATCH_WINDOW_MS = 100;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;
    private static final long HASH_CACHE_EXPIRY_HOURS = 24;

    public @interface WatchServiceConfiguration {
        String name() default "";
        String path() default "";
    }

    private final Logger logger = LoggerFactory.getLogger(WatchServiceImpl.class);
    private final BlockingQueue<DirectoryChangeEvent> eventQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_SIZE);
    private final List<Listener> dirPathListeners = new CopyOnWriteArrayList<>();
    private final List<Listener> subDirPathListeners = new CopyOnWriteArrayList<>();
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private final String name;
    private final BundleContext bundleContext;
    private final AtomicReference<Path> basePath = new AtomicReference<>();
    private final Cache<Path, FileHash> hashCache;

    private @Nullable DirectoryWatcher dirWatcher;
    private @Nullable ServiceRegistration<WatchService> reg;
    private @Nullable ScheduledFuture<?> batchProcessingTask;

    @Activate
    public WatchServiceImpl(WatchServiceConfiguration config, BundleContext bundleContext) throws IOException {
        this.bundleContext = bundleContext;
        if (config.name().isBlank()) {
            throw new IllegalArgumentException("service name must not be blank");
        }

        this.hashCache = CacheBuilder.newBuilder()
            .maximumSize(HASH_CACHE_MAX_SIZE)
            .expireAfterAccess(HASH_CACHE_EXPIRY_HOURS, TimeUnit.HOURS)
            .recordStats()
            .build();

        this.name = config.name();
        executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1000),
            r -> new Thread(r, name + "-executor"),
            new ThreadPoolExecutor.CallerRunsPolicy());

        scheduler = ThreadPoolManager.getScheduledPool("watchservice");
        modified(config);
        
        batchProcessingTask = scheduler.scheduleWithFixedDelay(
            this::processBatchEvents,
            BATCH_PROCESSING_INTERVAL,
            BATCH_PROCESSING_INTERVAL,
            TimeUnit.MILLISECONDS
        );
    }

    @Modified
    public void modified(WatchServiceConfiguration config) throws IOException {
        logger.trace("Trying to setup WatchService '{}' with path '{}'", config.name(), config.path());
        Path newBasePath = Path.of(config.path()).toAbsolutePath();
        Path currentBasePath = basePath.get();

        if (newBasePath.equals(currentBasePath)) {
            return;
        }

        try {
            closeWatcherAndUnregister();
            ensureDirectoryExists(newBasePath);
            basePath.set(newBasePath);

            DirectoryWatcher newDirWatcher = DirectoryWatcher.builder()
                .listener(this)
                .path(newBasePath)
                .build();

            CompletableFuture
                .runAsync(
                    () -> newDirWatcher.watchAsync(executor)
                        .thenRun(() -> logger.debug("WatchService '{}' has been shut down.", name)),
                    ThreadPoolManager.getScheduledPool(ThreadPoolManager.THREAD_POOL_NAME_COMMON))
                .thenRun(this::registerWatchService);

            this.dirWatcher = newDirWatcher;

        } catch (NoSuchFileException e) {
            logger.warn("Could not instantiate WatchService '{}', directory '{}' is missing.", name, e.getMessage());
            throw e;
        } catch (IOException e) {
            logger.warn("Could not instantiate WatchService '{}':", name, e);
            throw e;
        }
    }

    private void ensureDirectoryExists(Path path) throws IOException {
        int attempts = 0;
        while (attempts < MAX_RETRY_ATTEMPTS) {
            try {
                if (!Files.exists(path)) {
                    Files.createDirectories(path);
                }
                return;
            } catch (IOException e) {
                attempts++;
                if (attempts == MAX_RETRY_ATTEMPTS) {
                    throw e;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Directory creation interrupted", ie);
                }
            }
        }
    }

    @Deactivate
    public void deactivate() {
        try {
            if (batchProcessingTask != null) {
                batchProcessingTask.cancel(false);
            }

            closeWatcherAndUnregister();
            
            executor.shutdown();
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            
            scheduler.shutdown();
            if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Failed to shutdown WatchService '{}'", name, e);
        }
    }

    private void closeWatcherAndUnregister() throws IOException {
        DirectoryWatcher localDirWatcher = this.dirWatcher;
        if (localDirWatcher != null) {
            localDirWatcher.close();
            this.dirWatcher = null;
        }

        ServiceRegistration<?> localReg = this.reg;
        if (localReg != null) {
            try {
                localReg.unregister();
            } catch (IllegalStateException e) {
                logger.debug("WatchService '{}' was already unregistered.", name, e);
            }
            this.reg = null;
        }

        hashCache.invalidateAll();
        eventQueue.clear();
    }

    private void registerWatchService() {
        Dictionary<String, Object> properties = new Hashtable<>();
        properties.put(WatchService.SERVICE_PROPERTY_NAME, name);
        this.reg = bundleContext.registerService(WatchService.class, this, properties);
        logger.debug("WatchService '{}' completed initialization and registered itself as service.", name);
    }

    @Override
    public Path getWatchPath() {
        Path path = basePath.get();
        if (path == null) {
            throw new IllegalStateException("Trying to access WatchService before initialization completed.");
        }
        return path;
    }

    @Override
    public void registerListener(WatchEventListener watchEventListener, List<Path> paths, boolean withSubDirectories) {
        Path path = basePath.get();
        if (path == null) {
            throw new IllegalStateException("Trying to register listener before initialization completed.");
        }
        
        for (Path listenerPath : paths) {
            Path absolutePath = listenerPath.isAbsolute() ? listenerPath : path.resolve(listenerPath).toAbsolutePath();
            if (absolutePath.startsWith(path)) {
                if (withSubDirectories) {
                    subDirPathListeners.add(new Listener(absolutePath, watchEventListener));
                } else {
                    dirPathListeners.add(new Listener(absolutePath, watchEventListener));
                }
            } else {
                logger.warn("Tried to add path '{}' to listener '{}', but the base path of this listener is '{}'",
                    listenerPath, name, path);
            }
        }
    }

    @Override
    public void unregisterListener(WatchEventListener watchEventListener) {
        subDirPathListeners.removeIf(Listener.isListener(watchEventListener));
        dirPathListeners.removeIf(Listener.isListener(watchEventListener));
    }

    private void processBatchEvents() {
        try {
            List<DirectoryChangeEvent> batch = new ArrayList<>(EVENT_BATCH_SIZE);
            DirectoryChangeEvent event;
            long deadline = System.currentTimeMillis() + BATCH_WINDOW_MS;

            while (System.currentTimeMillis() < deadline && batch.size() < EVENT_BATCH_SIZE) {
                event = eventQueue.poll();
                if (event == null) break;
                batch.add(event);
            }

            if (batch.isEmpty()) {
                return;
            }

            Map<Path, List<DirectoryChangeEvent>> eventsByPath = batch.stream()
                .collect(Collectors.groupingBy(DirectoryChangeEvent::path,
                    Collectors.collectingAndThen(
                        Collectors.toList(),
                        list -> {
                            list.sort(Comparator.comparing(DirectoryChangeEvent::timestamp));
                            return list;
                        }
                    )));

            for (Map.Entry<Path, List<DirectoryChangeEvent>> entry : eventsByPath.entrySet()) {
                Path path = entry.getKey();
                List<DirectoryChangeEvent> events = entry.getValue();
                processEventsForPath(path, events);
            }
        } catch (Exception e) {
            logger.error("Error processing batch events", e);
        }
    }

    private void processEventsForPath(Path path, List<DirectoryChangeEvent> events) {
        DirectoryChangeEvent firstEvent = events.get(0);
        DirectoryChangeEvent lastEvent = events.get(events.size() - 1);

        if (lastEvent.eventType() == DirectoryChangeEvent.EventType.DELETE) {
            if (firstEvent.eventType() != DirectoryChangeEvent.EventType.CREATE) {
                hashCache.invalidate(path);
                doNotify(path, Kind.DELETE);
            } else {
                logger.debug("Discarding events for '{}' as file was deleted right after creation", path);
            }
        } else {
            FileHash newHash = lastEvent.hash();
            if (newHash != null) {
                FileHash oldHash = hashCache.asMap().put(path, newHash);
                if (!Objects.equals(oldHash, newHash)) {
                    Kind kind = firstEvent.eventType() == DirectoryChangeEvent.EventType.CREATE 
                        ? Kind.CREATE : Kind.MODIFY;
                    doNotify(path, kind);
                }
            } else {
                logger.warn("Received event without hash for {}", path);
            }
        }
    }

    @Override
    public void onEvent(@Nullable DirectoryChangeEvent event) throws IOException {
        if (event == null || event.isDirectory() || 
            event.eventType() == DirectoryChangeEvent.EventType.OVERFLOW ||
            isTempFile(event.path())) {
            return;
        }

        if (!eventQueue.offer(event)) {
            logger.warn("Event queue full, dropping event for {}", event.path());
        }
    }

    private boolean isTempFile(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.startsWith(".") || 
               fileName.endsWith(".tmp") ||
               fileName.endsWith(".swp");
    }

    private void doNotify(Path path, Kind kind) {
        logger.trace("Notifying listeners of '{}' event for '{}'.", kind, path);
        subDirPathListeners.stream().filter(isChildOf(path)).forEach(l -> l.notify(path, kind));
        dirPathListeners.stream().filter(isDirectChildOf(path)).forEach(l -> l.notify(path, kind));
    }

    public static Predicate<Listener> isChildOf(Path path) {
        return l -> path.startsWith(l.rootPath);
    }

    public static Predicate<Listener> isDirectChildOf(Path path) {
        return l -> path.startsWith(l.rootPath) && l.rootPath.relativize(path).getNameCount() == 1;
    }

    private record Listener(Path rootPath, WatchEventListener watchEventListener) {
        void notify(Path path, Kind kind) {
            watchEventListener.processWatchEvent(kind, rootPath.relativize(path));
        }

        static Predicate<Listener> isListener(WatchEventListener watchEventListener) {
            return l -> watchEventListener.equals(l.watchEventListener);
        }
    }
}
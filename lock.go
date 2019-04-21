package dlock

import (
	"time"

	"github.com/jinzhu/gorm"
)

type LockKey struct {
	Key        string `gorm:"primary_key"`
	LockTime   time.Time
	ExpireTime time.Time
}

type DLock struct {
	db *gorm.DB
}

func NewDLock(db *gorm.DB) *DLock {
	db.AutoMigrate(&LockKey{})
	return &DLock{db}
}

func (lock *DLock) Lock(key string, duration time.Duration) bool {
	if insertLockKey(lock.db, key, duration) {
		return true
	}

	return updateLockKey(lock.db, key, duration)
}

func (lock *DLock) UnLock(key string) bool {
	return deleteLockKey(lock.db, key)
}

func insertLockKey(db *gorm.DB, key string, duration time.Duration) bool {
	now := time.Now()
	return db.Create(&LockKey{Key: key, ExpireTime: now.Add(duration), LockTime: now}).Error == nil

}

func getLockKey(db *gorm.DB, key string) *LockKey {
	lockKey := &LockKey{}
	if db.Where("key = ?", key).First(lockKey).RecordNotFound() {
		return nil
	}
	return lockKey
}

func updateLockKey(db *gorm.DB, key string, duration time.Duration) bool {
	oldKey := getLockKey(db, key)
	if oldKey == nil {
		return false
	}
	if time.Now().Before(oldKey.ExpireTime) {
		return false
	}
	now := time.Now()
	return db.Model(&LockKey{}).Where("key = ? AND lock_time = ?", key, oldKey.LockTime).UpdateColumns(map[string]interface{}{"lock_time": now, "expire_time": now.Add(duration)}).RowsAffected == 1
}

func deleteLockKey(db *gorm.DB, key string) bool {
	return db.Where("key = ?", key).Delete(&LockKey{}).RowsAffected == 1
}
